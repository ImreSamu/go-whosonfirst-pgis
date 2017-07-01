package pgis

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-crawl"
	"github.com/whosonfirst/go-whosonfirst-csv"
	"github.com/whosonfirst/go-whosonfirst-geojson"
	"github.com/whosonfirst/go-whosonfirst-placetypes"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"github.com/whosonfirst/go-whosonfirst-utils"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Meta struct {
	Name      string           `json:"wof:name"`
	Country   string           `json:"wof:country"`
	Repo      string           `json:"wof:repo"`
	Hierarchy []map[string]int `json:"wof:hierarchy"`
}

type Coords []float64

type LinearRing []Coords

type Polygon []LinearRing

type MultiPolygon []Polygon

type Geometry struct {
	Type        string `json:"type"`
	Coordinates Coords `json:"coordinates"`
}

type GeometryPoly struct {
	Type        string  `json:"type"`
	Coordinates Polygon `json:"coordinates"`
}

type GeometryMultiPoly struct {
	Type        string       `json:"type"`
	Coordinates MultiPolygon `json:"coordinates"`
}

type PgisRow struct {
	Id           int64
	ParentId     int64
	PlacetypeId  int64
	IsSuperseded int
	IsDeprecated int
	Meta         string
	Geom         string
	Centroid     string
}

// this is here so we can pass both sql.Row and sql.Rows to the
// QueryRowToPgisRow function below (20170630/thisisaaronland)

type PgisResultSet interface {
	Scan(dest ...interface{}) error
}

type PgisIntersectsOptions struct {
	PlacetypeId  int64
	IsSuperseded int
	IsDeprecated int
	NumProcesses int
	PerPage      int
}

func NewDefaultPgisIntersectsOptions() *PgisIntersectsOptions {

	opts := PgisIntersectsOptions{
		PlacetypeId:  -1,
		IsSuperseded: 0,
		IsDeprecated: 0,
		NumProcesses: 4,
		PerPage:      1000,
	}

	return &opts
}

func NewPgisRow(id int64, pid int64, ptid int64, superseded int, deprecated int, meta string, geom string, centroid string) (*PgisRow, error) {

	row := PgisRow{
		Id:           id,
		ParentId:     pid,
		PlacetypeId:  ptid,
		IsSuperseded: superseded,
		IsDeprecated: deprecated,
		Meta:         meta,
		Geom:         geom,
		Centroid:     centroid,
	}

	return &row, nil
}

func (row *PgisRow) GeomHash() (string, error) {

	return utils.HashFromJSON([]byte(row.Geom))
}

type PgisClient struct {
	Geometry string
	Debug    bool
	Verbose  bool
	dsn      string
	db       *sql.DB
	conns    chan bool
}

func NewPgisClient(host string, port int, user string, password string, dbname string, maxconns int) (*PgisClient, error) {

	var dsn string

	if password == "" {
		dsn = fmt.Sprintf("host=%s port=%d user=%s dbname=%s sslmode=disable", host, port, user, dbname)
	} else {
		dsn = fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	}

	db, err := sql.Open("postgres", dsn)

	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(512)
	db.SetMaxOpenConns(1024)

	// defer db.Close()

	err = db.Ping()

	if err != nil {
		return nil, err
	}

	conns := make(chan bool, maxconns)

	for i := 0; i < maxconns; i++ {
		conns <- true
	}

	client := PgisClient{
		Geometry: "", // use the default geojson geometry
		Debug:    false,
		dsn:      dsn,
		db:       db,
		conns:    conns,
	}

	return &client, nil
}

func (client *PgisClient) dbconn() (*sql.DB, error) {

	<-client.conns

	return client.db, nil
}

func (client *PgisClient) Connection() (*sql.DB, error) {

	<-client.conns

	return client.db, nil
}

func (client *PgisClient) IntersectsFeature(f []byte, opts *PgisIntersectsOptions) ([]*PgisRow, error) {

	rows := make([]*PgisRow, 0)

	geom := gjson.GetBytes(f, "geometry")

	if !geom.Exists() {
		err := errors.New("Feature is missing a geometry")
		return nil, err
	}

	str_geom := geom.String()

	sql_geom := "SELECT COUNT(id) FROM whosonfirst WHERE ST_Intersects(ST_GeomFromGeoJSON($1), geom) AND is_superseded=$2 AND is_deprecated=$3 AND placetype_id=$4"
	sql_centroid := "SELECT COUNT(id) FROM whosonfirst WHERE ST_Intersects(ST_GeomFromGeoJSON($1), centroid) AND geom IS NULL AND is_superseded=$2 AND is_deprecated=$3 AND placetype_id=$4"

	count_geom := 0
	count_centroid := 0
	count_total := 0

	geom_ch := make(chan int)
	centroid_ch := make(chan int)

	err_ch := make(chan error)
	done_ch := make(chan bool)

	t1 := time.Now()

	get_count := func(sql string, str_geom string, opts *PgisIntersectsOptions, count_ch chan int, done_ch chan bool, err_ch chan error) {

		defer func() {
			done_ch <- true
		}()

		count_rows, err := client.CountIntersects(sql, str_geom, opts)

		if err != nil {
			err_ch <- err
			return
		}

		count_ch <- count_rows
	}

	go get_count(sql_geom, str_geom, opts, geom_ch, done_ch, err_ch)
	go get_count(sql_centroid, str_geom, opts, centroid_ch, done_ch, err_ch)

	for n := 2; n > 0; {
		select {
		case c := <-geom_ch:
			count_geom = c
			count_total += c
		case c := <-centroid_ch:
			count_centroid = c
			count_total += c
		case err := <-err_ch:
			return nil, err
		case <-done_ch:
			n--
		}
	}

	t2 := time.Since(t1)

	log.Println("COUNT GEOM", count_geom)
	log.Println("COUNT CENTROIDS", count_centroid)
	log.Println("COUNT", count_total)
	log.Printf("TIME total %v\n", t2)

	t3 := time.Now()

	// ./bin/wof-pgis-intersects -placetype neighbourhood -pgis-user postgres -pgis-host locahost /usr/local/data/whosonfirst-data/data/859/225/83/85922583.geojson

	rows_ch := make(chan *PgisRow)
	fetching := 0

	cols := client.PgisRowQueryColumns()

	if count_geom > 0 {

		sql := fmt.Sprintf("SELECT %s FROM whosonfirst WHERE ST_Intersects(ST_GeomFromGeoJSON($1), geom) AND is_superseded=$2 AND is_deprecated=$3 AND placetype_id=$4 OFFSET $5 LIMIT $6", cols)

		client.FetchIntersectsAsync(sql, count_geom, str_geom, opts, rows_ch, done_ch, err_ch)
		fetching += 1
	}

	if count_centroid > 0 {

		sql := fmt.Sprintf("SELECT %s FROM whosonfirst WHERE ST_Intersects(ST_GeomFromGeoJSON($1), centroid) AND geom IS NULL AND is_superseded=$2 AND is_deprecated=$3 AND placetype_id=$4 OFFSET $5 LIMIT $6", cols)

		client.FetchIntersectsAsync(sql, count_centroid, str_geom, opts, rows_ch, done_ch, err_ch)
		fetching += 1
	}

	for f := fetching; f > 0; {
		select {
		case pg_row := <-rows_ch:
			rows = append(rows, pg_row)
		case err := <-err_ch:
			// KILL ALL THE OTHER CHANNELS...
			return nil, err
		case <-done_ch:
			f--
		}
	}

	t4 := time.Since(t3)
	log.Printf("TIME %v\n", t4)

	t5 := time.Since(t1)
	log.Printf("TIME ALL %v\n", t5)

	return rows, nil
}

func (client *PgisClient) CountIntersects(sql string, str_geom string, opts *PgisIntersectsOptions) (int, error) {

	db, err := client.dbconn()

	if err != nil {
		return -1, err
	}

	// ta := time.Now()

	row := db.QueryRow(sql, str_geom, opts.IsSuperseded, opts.IsDeprecated, opts.PlacetypeId)

	// tb := time.Since(ta)
	// log.Printf("TIME %s %v\n", sql, tb)

	var count_rows int
	err = row.Scan(&count_rows)

	if err != nil {
		return -1, err
	}

	return count_rows, nil
}

func (client *PgisClient) FetchIntersectsAsync(sql string, count_expected int, str_geom string, opts *PgisIntersectsOptions, rows_ch chan *PgisRow, done_ch chan bool, err_ch chan error) {


        defer func(){
		done_ch <- true
	}()
	
	db, err := client.dbconn()

	if err != nil {
	   	err_ch <- err
		return
	}

	limit := opts.PerPage

	count_fl := float64(count_expected)
	limit_fl := float64(limit)

	iters_fl := count_fl / limit_fl
	iters_fl = math.Ceil(iters_fl)
	iters := int(iters_fl)

	count_throttle := opts.NumProcesses

	throttle_ch := make(chan bool, count_throttle)
	fetch_ch := make(chan bool)
	
	for t := 0; t < count_throttle; t++ {
		throttle_ch <- true
	}

	for offset := 0; offset <= count_expected; offset += limit {

		go func(str_geom string, opts *PgisIntersectsOptions, offset int, limit int) {

			<-throttle_ch

			defer func() {
				fetch_ch <- true
				throttle_ch <- true
			}()

			// ta := time.Now()
			// log.Printf("[%d] %s %d/%d\n", x, s, offset, limit)

			r, err := db.Query(sql, str_geom, opts.IsSuperseded, opts.IsDeprecated, opts.PlacetypeId, offset, limit)

			// tb := time.Since(ta)
			// log.Printf("[%d] %s %v\n", x, s, tb)

			if err != nil {
				err_ch <- err
				return
			}

			defer r.Close()

			for r.Next() {

				pg_row, err := client.QueryRowToPgisRow(r)

				if err != nil {
					err_ch <- err
					return
				}

				rows_ch <- pg_row
			}

		}(str_geom, opts, offset, limit)

	}

	for i := iters; i > 0; {
		select {
		case <-fetch_ch:
			i--
		}
	}

}

func (client *PgisClient) GetById(id int64) (*PgisRow, error) {

	db, err := client.dbconn()

	if err != nil {
		return nil, err
	}

	sql := fmt.Sprintf("SELECT id, parent_id, placetype_id, is_superseded, is_deprecated, meta, ST_AsGeoJSON(geom), ST_AsGeoJSON(centroid) FROM whosonfirst WHERE id=$1")
	row := db.QueryRow(sql, id)

	pg_row, err := client.QueryRowToPgisRow(row)

	if err != nil {
		return nil, err
	}

	return pg_row, nil
}

func (client *PgisClient) IndexFile(abs_path string, collection string) error {

	// check to see if this is an alt file
	// https://github.com/whosonfirst/go-whosonfirst-tile38/issues/1

	feature, err := geojson.UnmarshalFile(abs_path)

	if err != nil {
		return err
	}

	return client.IndexFeature(feature, collection)
}

func (client *PgisClient) IndexFeature(feature *geojson.WOFFeature, collection string) error {

	wofid := feature.Id()

	if wofid == 0 {
		log.Println("skipping Earth because it confused PostGIS")
		return nil
	}

	str_wofid := strconv.Itoa(wofid)

	body := feature.Body()

	str_geom := ""
	str_centroid := ""

	if client.Geometry == "" {

		var geom_type string
		geom_type, _ = body.Path("geometry.type").Data().(string)

		if geom_type == "MultiPolygon" {

			geom := body.Path("geometry")
			str_geom = geom.String()

		} else if geom_type == "Polygon" {

			polys := make([]Polygon, 0)

			for _, p := range feature.GeomToPolygons() {

				poly := make([]LinearRing, 0)
				ring := make([]Coords, 0)

				for _, po := range p.OuterRing.Points() {

					coord := Coords{po.Lng(), po.Lat()}
					ring = append(ring, coord)
				}

				poly = append(poly, ring)

				for _, pi := range p.InteriorRings {

					ring := make([]Coords, 0)

					for _, pt := range pi.Points() {

						coord := Coords{pt.Lng(), pt.Lat()}
						ring = append(ring, coord)
					}

					poly = append(poly, ring)
				}

				polys = append(polys, poly)
			}

			multi := GeometryMultiPoly{
				Type:        "MultiPolygon",
				Coordinates: polys,
			}

			geom, _ := json.Marshal(multi)
			str_geom = string(geom)

		} else if geom_type == "Point" {

			geom := body.Path("geometry")
			str_centroid = geom.String()

			// note we are leaving str_geom empty since it is not
			// a multipolygon... because postgis

		} else {
			log.Println("GEOM TYPE IS", geom_type)
			return errors.New("DO SOMETHING HERE")
		}

	} else if client.Geometry == "bbox" {

		/*

			This is not really the best way to deal with the problem since
			we'll end up with an oversized bounding box. A better way would
			be to store the bounding box for each polygon in the geom and
			flag that in the key name. Which is easy but just requires tweaking
			a few things and really I just want to see if this works at all
			from a storage perspective right now (20160902/thisisaaronland)

		*/

		var swlon float64
		var swlat float64
		var nelon float64
		var nelat float64

		children, _ := body.S("bbox").Children()

		swlon = children[0].Data().(float64)
		swlat = children[1].Data().(float64)
		nelon = children[2].Data().(float64)
		nelat = children[3].Data().(float64)

		ring := LinearRing{
			Coords{swlon, swlat},
			Coords{swlon, nelat},
			Coords{nelon, nelat},
			Coords{nelon, swlat},
			Coords{swlon, swlat},
		}

		poly := Polygon{ring}

		geom := GeometryPoly{
			Type:        "Polygon",
			Coordinates: poly,
		}

		bytes, err := json.Marshal(geom)

		if err != nil {
			return err
		}

		str_geom = string(bytes)

	} else if client.Geometry == "centroid" {
		// handled below
	} else {
		return errors.New("unknown geometry filter")
	}

	if str_centroid == "" {

		// sudo put me in go-whosonfirst-geojson?
		// (20160829/thisisaaronland)

		var lat float64
		var lon float64
		var lat_ok bool
		var lon_ok bool

		lat, lat_ok = body.Path("properties.lbl:latitude").Data().(float64)
		lon, lon_ok = body.Path("properties.lbl:longitude").Data().(float64)

		if !lat_ok || !lon_ok {

			lat, lat_ok = body.Path("properties.geom:latitude").Data().(float64)
			lon, lon_ok = body.Path("properties.geom:longitude").Data().(float64)
		}

		if !lat_ok || !lon_ok {
			return errors.New("can't find centroid")
		}

		coords := Coords{lon, lat}

		geom := Geometry{
			Type:        "Point",
			Coordinates: coords,
		}

		bytes, err := json.Marshal(geom)

		if err != nil {
			return err
		}

		str_centroid = string(bytes)
	}

	placetype := feature.Placetype()

	pt, err := placetypes.GetPlacetypeByName(placetype)

	if err != nil {
		return err
	}

	repo, ok := feature.StringProperty("wof:repo")

	if !ok {
		msg := fmt.Sprintf("can't find wof:repo for %s", str_wofid)
		return errors.New(msg)
	}

	if repo == "" {

		msg := fmt.Sprintf("missing wof:repo for %s", str_wofid)
		return errors.New(msg)
	}

	key := str_wofid + "#" + repo

	parent, ok := feature.IntProperty("wof:parent_id")

	if !ok {
		log.Printf("FAILED to determine parent ID for %s\n", key)
		parent = -1
	}

	is_superseded := 0
	is_deprecated := 0

	if feature.Deprecated() {
		is_deprecated = 1
	}

	if feature.Superseded() {
		is_superseded = 1
	}

	meta_key := str_wofid + "#meta"

	name := feature.Name()
	country, ok := feature.StringProperty("wof:country")

	if !ok {
		log.Printf("FAILED to determine country for %s\n", meta_key)
		country = "XX"
	}

	hier := feature.Hierarchy()

	meta := Meta{
		Name:      name,
		Country:   country,
		Hierarchy: hier,
		Repo:      repo,
	}

	meta_json, err := json.Marshal(meta)

	if err != nil {
		log.Printf("FAILED to marshal JSON on %s because, %v\n", meta_key, err)
		return err
	}

	str_meta := string(meta_json)

	f := []byte(feature.Dumps())
	geom_hash, err := utils.HashGeomFromFeature(f)

	if err != nil {
		log.Printf("FAILED to hash geom because, %s\n", err)
		return err
	}

	now := time.Now()
	lastmod := now.Format(time.RFC3339)

	// http://postgis.net/docs/ST_GeomFromGeoJSON.html

	st_geojson := fmt.Sprintf("ST_GeomFromGeoJSON('%s')", str_geom)
	st_centroid := fmt.Sprintf("ST_GeomFromGeoJSON('%s')", str_centroid)

	if client.Verbose {

		// because we might be in verbose mode but not debug mode
		// so the actual GeoJSON blob needs to be preserved

		actual_st_geojson := st_geojson

		if client.Geometry == "" {
			st_geojson = "ST_GeomFromGeoJSON('...')"
		}

		log.Println("INSERT INTO whosonfirst (id, parent_id, placetype_id, is_superseded, is_deprecated, meta, geom_hash, lastmod, geom, centroid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)", wofid, parent, pt.Id, is_superseded, is_deprecated, str_meta, geom_hash, lastmod, st_geojson, st_centroid)

		st_geojson = actual_st_geojson
	}

	if !client.Debug {

		db, err := client.dbconn()

		if err != nil {
			return err
		}

		defer func() {
			client.conns <- true
		}()

		// https://www.postgresql.org/docs/9.6/static/sql-insert.html#SQL-ON-CONFLICT
		// https://wiki.postgresql.org/wiki/What's_new_in_PostgreSQL_9.5#INSERT_..._ON_CONFLICT_DO_NOTHING.2FUPDATE_.28.22UPSERT.22.29

		var sql string

		if str_geom != "" && str_centroid != "" {

			sql = fmt.Sprintf("INSERT INTO whosonfirst (id, parent_id, placetype_id, is_superseded, is_deprecated, meta, geom_hash, lastmod, geom, centroid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, %s, %s) ON CONFLICT(id) DO UPDATE SET parent_id=$9, placetype_id=$10, is_superseded=$11, is_deprecated=$12, meta=$13, geom_hash=$14, lastmod=$15, geom=%s, centroid=%s", st_geojson, st_centroid, st_geojson, st_centroid)

		} else if str_geom != "" {

			sql = fmt.Sprintf("INSERT INTO whosonfirst (id, parent_id, placetype_id, is_superseded, is_deprecated, meta, geom_hash, lastmod, xgeom, centroid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, %s) ON CONFLICT(id) DO UPDATE SET parent_id=$9, placetype_id=$10, is_superseded=$11, is_deprecated=$12, meta=$13, geom_hash=$14, lastmod=$15, geom=%s", st_geojson, st_geojson)

		} else if str_centroid != "" {

			sql = fmt.Sprintf("INSERT INTO whosonfirst (id, parent_id, placetype_id, is_superseded, is_deprecated, meta, geom_hash, lastmod, centroid) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, %s) ON CONFLICT(id) DO UPDATE SET parent_id=$9, placetype_id=$10, is_superseded=$11, is_deprecated=$12, meta=$13, geom_hash=$14, lastmod=$15, centroid=%s", st_centroid, st_centroid)

		} else {
			// this should never happend
		}

		_, err = db.Exec(sql, wofid, parent, pt.Id, is_superseded, is_deprecated, str_meta, geom_hash, lastmod, parent, pt.Id, is_superseded, is_deprecated, str_meta, geom_hash, lastmod)

		if err != nil {

			log.Println(err)
			log.Println(sql)
			os.Exit(1)
			return err
		}

		/*
			rows, _ := rsp.RowsAffected()
			log.Println("ERR", err)
			log.Println("ROWS", rows)
		*/
	}

	return nil

}

func (client *PgisClient) IndexMetaFile(csv_path string, collection string, data_root string) error {

	reader, err := csv.NewDictReaderFromPath(csv_path)

	if err != nil {
		return err
	}

	count := runtime.GOMAXPROCS(0) // perversely this is how we get the count...
	ch := make(chan bool, count)

	go func() {
		for i := 0; i < count; i++ {
			ch <- true
		}
	}()

	wg := new(sync.WaitGroup)

	for {
		row, err := reader.Read()

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}

		rel_path, ok := row["path"]

		if !ok {
			msg := fmt.Sprintf("missing 'path' column in meta file")
			return errors.New(msg)
		}

		abs_path := filepath.Join(data_root, rel_path)

		<-ch

		wg.Add(1)

		go func(ch chan bool) {

			defer func() {
				wg.Done()
				ch <- true
			}()

			client.IndexFile(abs_path, collection)

		}(ch)
	}

	wg.Wait()

	return nil
}

func (client *PgisClient) IndexDirectory(abs_path string, collection string, nfs_kludge bool) error {

	re_wof, _ := regexp.Compile(`(\d+)\.geojson$`)

	count := 0
	ok := 0
	errs := 0

	cb := func(abs_path string, info os.FileInfo) error {

		// please make me more like this...
		// https://github.com/whosonfirst/py-mapzen-whosonfirst-utils/blob/master/mapzen/whosonfirst/utils/__init__.py#L265

		fname := filepath.Base(abs_path)

		if !re_wof.MatchString(fname) {
			// log.Println("skip", abs_path)
			return nil
		}

		count += 1

		err := client.IndexFile(abs_path, collection)

		if err != nil {
			errs += 1
			msg := fmt.Sprintf("failed to index %s, because %v", abs_path, err)
			log.Println(msg)
			return errors.New(msg)
		}

		ok += 1
		return nil
	}

	c := crawl.NewCrawler(abs_path)
	c.NFSKludge = nfs_kludge

	c.Crawl(cb)

	log.Printf("count %d ok %d error %d\n", count, ok, errs)
	return nil
}

func (client *PgisClient) IndexFileList(abs_path string, collection string) error {

	file, err := os.Open(abs_path)

	if err != nil {
		return err
	}

	defer file.Close()

	scanner := bufio.NewScanner(file)

	count := runtime.GOMAXPROCS(0) // perversely this is how we get the count...
	ch := make(chan bool, count)

	go func() {
		for i := 0; i < count; i++ {
			ch <- true
		}
	}()

	wg := new(sync.WaitGroup)

	for scanner.Scan() {

		<-ch

		path := scanner.Text()

		wg.Add(1)

		go func(path string, collection string, wg *sync.WaitGroup, ch chan bool) {

			defer wg.Done()

			client.IndexFile(path, collection)
			ch <- true

		}(path, collection, wg, ch)
	}

	wg.Wait()

	return nil
}

func (client *PgisClient) Prune(data_root string, delete bool) error {

	db, err := client.dbconn()

	if err != nil {
		return err
	}

	sql_count := "SELECT COUNT(id) FROM whosonfirst"

	row := db.QueryRow(sql_count)

	var count_rows int
	err = row.Scan(&count_rows)

	if err != nil {
		return err
	}

	limit := 100000

	for offset := 0; offset < count_rows; offset += limit {

		sql := fmt.Sprintf("SELECT id, meta FROM whosonfirst OFFSET %d LIMIT %d", offset, limit)
		log.Printf("%s (%d)\n", sql, count_rows)

		rows, err := db.Query(sql)

		if err != nil {
			return err
		}

		count := runtime.GOMAXPROCS(0)
		throttle := make(chan bool, count)

		for i := 0; i < count; i++ {
			throttle <- true
		}

		wg := new(sync.WaitGroup)

		for rows.Next() {

			var wofid int64
			var str_meta string

			err := rows.Scan(&wofid, &str_meta)

			if err != nil {
				return err
			}

			<-throttle

			wg.Add(1)

			go func(data_root string, wofid int64, str_meta string, throttle chan bool) {

				defer func() {
					wg.Done()
					throttle <- true
				}()

				var meta Meta

				err := json.Unmarshal([]byte(str_meta), &meta)

				if err != nil {
					return
				}

				repo := filepath.Join(data_root, meta.Repo)
				data := filepath.Join(repo, "data")

				wof_path, err := uri.Id2AbsPath(data, wofid)

				if err != nil {
					return
				}

				_, err = os.Stat(wof_path)

				if !os.IsNotExist(err) {
					return
				}

				log.Printf("%s does not exist\n", wof_path)

				if delete {

					db, err := client.dbconn()

					if err != nil {
						return
					}

					defer func() {
						client.conns <- true
					}()

					sql := "DELETE FROM whosonfirst WHERE id=$1"
					_, err = db.Exec(sql, wofid)

					if err != nil {
						log.Println(sql, wofid, err)
					}
				}

			}(data_root, wofid, str_meta, throttle)
		}

		wg.Wait()
	}

	return nil
}

func (client *PgisClient) PgisRowColumns() []string {

	cols := []string{
		"id",
		"parent_id",
		"placetype_id",
		"is_superseded",
		"is_deprecated",
		"meta",
		"ST_AsGeoJSON(geom)",
		"ST_AsGeoJSON(centroid)",
	}

	return cols
}

func (client *PgisClient) PgisRowQueryColumns() string {

	cols := client.PgisRowColumns()
	return strings.Join(cols, ",")
}

func (client *PgisClient) QueryRowToPgisRow(row PgisResultSet) (*PgisRow, error) {

	var wofid int64
	var parentid int64
	var placetypeid int64
	var superseded int
	var deprecated int
	var meta string
	var geom string
	var centroid string

	err := row.Scan(&wofid, &parentid, &placetypeid, &superseded, &deprecated, &meta, &geom, &centroid)

	if err != nil {
		return nil, err
	}

	pgrow, err := NewPgisRow(wofid, parentid, placetypeid, superseded, deprecated, meta, geom, centroid)

	if err != nil {
		return nil, err
	}

	return pgrow, nil
}

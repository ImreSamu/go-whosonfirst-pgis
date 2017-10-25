package index

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/geometry"
	"github.com/whosonfirst/go-whosonfirst-geojson-v2/properties/whosonfirst"
	"github.com/whosonfirst/go-whosonfirst-placetypes"
	"github.com/whosonfirst/go-whosonfirst-tile38"
	"github.com/whosonfirst/go-whosonfirst-tile38/util"
	"github.com/whosonfirst/go-whosonfirst-uri"
	"log"
	"strconv"
)

// see notes inre go-whosonfirst-spr below

type Meta struct {
	Name    string `json:"wof:name"`
	Country string `json:"wof:country"`
}

type Coords []float64

type Polygon []Coords

type Geometry struct {
	Type        string `json:"type"`
	Coordinates Coords `json:"coordinates"`
}

type GeometryPoly struct {
	Type        string    `json:"type"`
	Coordinates []Polygon `json:"coordinates"`
}

type PgisIndexer struct {
	Geometry string
	Debug    bool
	Verbose  bool
	Strict   bool
	clients  []tile38.Tile38Client
}

func NewPgisIndexer(clients ...tile38.PgisClient) (*PgisIndexer, error) {

	idx := PgisIndexer{
		Geometry: "", // use the default geojson geometry
		Debug:    false,
		Verbose:  false,
		Strict:   true,
		clients:  clients,
	}

	return &idx, nil
}

func (idx *PgisIndexer) IndexFeature(feature geojson.Feature, collection string) error {

	wofid := wof.Id(feature)

	if wofid == 0 {
		client.Logger.Debug("skipping Earth because it confuses PostGIS")
		return nil
	}

	str_wofid := strconv.FormatInt(wofid, 10)

	geom_type := geom.Type(feature)

	str_geom, err := geom.ToString(feature)

	if err != nil {
		return err
	}

	// we do this now because we might redefine str_geom below (to
	// be "") if we are dealing with a Point geometry which will
	// cause the JSON wrangling in HashGeometry to fail
	// (20170823/thisisaaronland)

	geom_hash, err := utils.HashGeometry([]byte(str_geom))

	if err != nil {
		return err
	}

	centroid, err := wof.Centroid(feature)

	if err != nil {
		return err
	}

	// client.Logger.Status("Centroid for %d derived from %s", wofid, centroid.Source())

	str_centroid, err := centroid.ToString()

	if err != nil {
		return err
	}

	if geom_type == "Point" {
		str_centroid = str_geom
		str_geom = ""
	}

	placetype := wof.Placetype(feature)

	pt, err := placetypes.GetPlacetypeByName(placetype)

	if err != nil {
		return err
	}

	repo := wof.Repo(feature)

	if repo == "" {

		msg := fmt.Sprintf("missing wof:repo for %s", str_wofid)
		return errors.New(msg)
	}

	parent := wof.ParentId(feature)

	is_deprecated, err := wof.IsDeprecated(feature)

	if err != nil {
		return err
	}

	is_superseded, err := wof.IsSuperseded(feature)

	if err != nil {
		return err
	}

	str_deprecated := is_deprecated.StringFlag()
	str_superseded := is_superseded.StringFlag()

	meta_key := str_wofid + "#meta"

	name := wof.Name(feature)
	country := wof.Country(feature)

	hier := wof.Hierarchy(feature)

	meta := Meta{
		Name:      name,
		Country:   country,
		Hierarchy: hier,
		Repo:      repo,
	}

	meta_json, err := json.Marshal(meta)

	if err != nil {
		client.Logger.Warning("FAILED to marshal JSON on %s because, %v", meta_key, err)
		return err
	}

	str_meta := string(meta_json)

	now := time.Now()
	lastmod := now.Format(time.RFC3339)

	// http://www.postgis.org/docs/ST_Multi.html
	// http://postgis.net/docs/ST_GeomFromGeoJSON.html

	st_geojson := fmt.Sprintf("ST_Multi(ST_GeomFromGeoJSON('%s'))", str_geom)
	st_centroid := fmt.Sprintf("ST_GeomFromGeoJSON('%s')", str_centroid)

	if client.Verbose {

		// because we might be in verbose mode but not debug mode
		// so the actual GeoJSON blob needs to be preserved

		actual_st_geojson := st_geojson

		if client.Geometry == "" {
			st_geojson = "ST_Multi(ST_GeomFromGeoJSON('...'))"
		}

		client.Logger.Status("INSERT INTO whosonfirst (id, parent_id, placetype_id, is_superseded, is_deprecated, meta, geom_hash, lastmod, geom, centroid) VALUES (%d, %d, %d, %s, %s, %s, %s, %s, %s, %s)", wofid, parent, pt.Id, str_superseded, str_deprecated, str_meta, geom_hash, lastmod, st_geojson, st_centroid)

		st_geojson = actual_st_geojson
	}

	if !client.Debug {

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

		err = idx.Exec(sql, wofid, parent, pt.Id, str_superseded, str_deprecated, str_meta, geom_hash, lastmod, parent, pt.Id, str_superseded, str_deprecated, str_meta, geom_hash, lastmod)

		if err != nil {
			return err
		}
	}

	return nil
}

func (idx *PgisIndexer) EnsureWOF(abs_path string, allow_alt bool) bool {

	wof, err := uri.IsWOFFile(abs_path)

	if err != nil {
		log.Println(fmt.Sprintf("Failed to determine whether %s is a WOF file, because %s", abs_path, err))
		return false
	}

	if !wof {
		return false
	}

	alt, err := uri.IsAltFile(abs_path)

	if err != nil {
		log.Println(fmt.Sprintf("Failed to determine whether %s is an alt file, because %s", abs_path, err))
		return false
	}

	if alt && !allow_alt {
		return false
	}

	return true
}

func (idx *PgisIndexer) Exec(cmd string, args ...interface{}) error {

	err_ch := make(chan error)
	done_ch := make(chan bool)

	for _, client := range idx.clients {

		go func(err_ch chan error, done_ch chan bool, client pgis.PgisClient, cmd string, args ...interface{}) {

			defer func() {
				done_ch <- true
			}()

			db, err := client.dbconn()

			if err != nil {
				return err
			}

			defer func() {
				client.conns <- true
			}()

			_, err = db.Exec(sql, args...)

			if err != nil {

				client.Logger.Error("failed to execute query because %s", err)
				client.Logger.Debug("%s", sql)

				os.Exit(1)
				return err
			}

		}(err_ch, done_ch, client, cmd, args...)

	}

	pending := len(idx.clients)

	for pending > 0 {

		select {
		case err := <-err_ch:
			return err
		case <-done_ch:
			pending -= 1
		}
	}

	return nil
}

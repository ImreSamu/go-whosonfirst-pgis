package main

import (
	"flag"
	"github.com/whosonfirst/go-whosonfirst-pgis/index"
	"github.com/whosonfirst/go-whosonfirst-placetypes"
	"io/ioutil"
	"log"
	"os"
)

func main() {

	pgis_host := flag.String("pgis-host", "localhost", "The host of your PostgreSQL server.")
	pgis_port := flag.Int("pgis-port", 5432, "The port of your PostgreSQL server.")
	pgis_user := flag.String("pgis-user", "whosonfirst", "The name of your PostgreSQL user.")
	pgis_pswd := flag.String("pgis-password", "", "The password of your PostgreSQL user.")
	pgis_dbname := flag.String("pgis-database", "whosonfirst", "The name of your PostgreSQL database.")
	pgis_maxconns := flag.Int("pgis-maxconns", 10, "The maximum number of connections to use with your PostgreSQL database.")

	placetype := flag.String("placetype", "", "...")

	verbose := flag.Bool("verbose", false, "Be chatty about what's happening. This is automatically enabled if the -debug flag is set.")
	debug := flag.Bool("debug", false, "Go through all the motions but don't actually index anything.")
	// strict := flag.Bool("strict", false, "Throw fatal errors rather than warning when certain conditions fails.")

	flag.Parse()

	if *debug {
		*verbose = true
	}

	pt, err := placetypes.GetPlacetypeByName(*placetype)

	if err != nil {
		log.Fatal(err)
	}

	client, err := pgis.NewPgisClient(*pgis_host, *pgis_port, *pgis_user, *pgis_pswd, *pgis_dbname, *pgis_maxconns)

	if err != nil {
		log.Fatalf("failed to create PgisClient (%s:%d) because %v", *pgis_host, *pgis_port, err)
	}

	client.Verbose = *verbose
	client.Debug = *debug

	// please allow for STDIN too...

	for _, path := range flag.Args() {

		fh, err := os.Open(path)

		if err != nil {
			log.Fatal(err)
		}

		feature, err := ioutil.ReadAll(fh)

		if err != nil {
			log.Fatal(err)
		}

		opts := pgis.PgisIntersectsOptions{
			PlacetypeId:  pt.Id,
			IsSuperseded: false,
			IsDeprecated: false,
		}

		rows, err := client.IntersectsFeature(feature, &opts)

		if err != nil {
			log.Fatal(err)
		}

		for _, row := range rows {
			log.Println(row)
		}
	}

	os.Exit(0)
}

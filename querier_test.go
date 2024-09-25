package pgxcache_test

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgx-contrib/pgxcache"
)

func ExampleQuerier() {
	config, err := pgxpool.ParseConfig(os.Getenv("PGX_DATABASE_URL"))
	if err != nil {
		panic(err)
	}

	// create a new connection pool
	conn, err := pgxpool.NewWithConfig(context.TODO(), config)
	if err != nil {
		panic(err)
	}
	// close the connection when the function returns
	defer conn.Close()

	// create a new querier
	querier := &pgxcache.Querier{
		// set the default query options, which can be overridden by the query
		// -- @cache-max-rows 100
		// -- @cache-ttl 30s
		Options: &pgxcache.QueryOptions{
			MaxLifetime: 30 * time.Second,
			MaxRows:     1,
		},
		Cacher:  pgxcache.NewMemoryQueryCacher(),
		Querier: conn,
	}

	// create a new organization struct
	type Organization struct {
		Name string `db:"name"`
	}

	// fetch all the organizations
	rows, err := querier.Query(context.TODO(), "SELECT * FROM organization")
	if err != nil {
		panic(err)
	}
	// close the rows when the function returns
	defer rows.Close()

	for rows.Next() {
		organization, err := pgx.RowToStructByName[Organization](rows)
		if err != nil {
			panic(err)
		}

		fmt.Println(organization.Name)
	}
}

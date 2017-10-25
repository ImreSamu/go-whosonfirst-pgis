package flags

import (
	"github.com/whosonfirst/go-whosonfirst-pgis/client"
	"strings"
)

type Endpoints []string

func (e *Endpoints) String() string {
	return strings.Join(*e, "\n")
}

func (e *Endpoints) Set(value string) error {
	*e = append(*e, value)
	return nil
}

func (e *Endpoints) ToClients() ([]*pgis.PgisClient, error) {

	clients := make([]*pgis.PgisClient, 0)

	for _, dsn := range *e {

		cl, err := pgis.NewPgisClientWithDSN(dsn, 10)

		if err != nil {
			return nil, err
		}

		clients = append(clients, cl)
	}

	return clients, nil
}

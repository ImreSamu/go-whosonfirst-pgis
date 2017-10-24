package flags

import (
	"errors"
	"fmt"
	"github.com/whosonfirst/go-whosonfirst-pgis/client"
	"strconv"
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

func (e *Endpoints) ToClients() ([]*client.PgisClient, error) {

	clients := make([]*client.PgisClient, 0)

	return clients, nil
}

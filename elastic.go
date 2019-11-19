package elasticsearchutil

import (
	"errors"

	"gopkg.in/olivere/elastic.v6"
)

// GetClient returns the first configured elasticsearch client
func GetClient() (*elastic.Client, error) {
	if len(elasticClients) == 0 {
		return nil, errors.New("failed to retrieve elasticsearch client")
	}

	return elasticClients[0], nil
}

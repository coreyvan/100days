package main

import (
	"github.com/elastic/go-elasticsearch/v7"
)

// ES .
type ES struct {
	Client elasticsearch.Client
}

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v7"
)

func main() {
	// should usually be "cse-cic-ids2018"
	bucket := os.Args[1]
	// should usually be "Processed Traffic Data for ML Algorithms/Friday-02-03-2018_TrafficForML_CICFlowMeter.csv"
	key := os.Args[2]
	//64KB is a reasonable amount
	kbs, err := strconv.Atoi(os.Args[3])
	if err != nil {
		log.Fatal("kb arg should be an integer", err)
	}
	readBytes := kbs * 1024
	// fileName := os.Args[4]

	// Configure AWS client
	sess, err := session.NewSession(&aws.Config{
		Region:      aws.String("ca-central-1"),
		Credentials: credentials.NewSharedCredentials("", "personal"),
	})
	if err != nil {
		log.Fatal("could not create session", err)
	}

	_, err = sess.Config.Credentials.Get()
	if err != nil {
		log.Fatal("could not set credentials", err)
	}

	c := S3Client{Session: sess}

	// Configure Elasticsearch client
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
		},
	}
	ec, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatal("could not create elasticsearch client", err)
	}

	res, err := ec.Info()
	if err != nil {
		log.Fatal("could not get cluster info", err)
	}

	if res.IsError() {
		log.Fatal("Error:", res.String())
	}

	// Download data from S3
	abuf := aws.NewWriteAtBuffer([]byte{})
	numBytes, err := c.getS3Data(
		abuf,
		&s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Range:  aws.String(fmt.Sprintf("bytes=0-%d", readBytes)),
		},
		64,
	)
	if err != nil {
		log.Fatal("could not download file", err)
	}

	// Split rows by \n
	rows := bytes.Split(abuf.Bytes(), []byte("\n"))

	log.Println("Downloaded", numBytes, "bytes")

	var r map[string]interface{}
	var wg sync.WaitGroup
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("Client: %s", elasticsearch.Version)
	log.Printf("Server: %s", r["version"].(map[string]interface{})["number"])
	log.Println(strings.Repeat("~", 37))

	for i, row := range rows {
		//Skip header row
		if i == 0 {
			continue
		}
		wg.Add(1)

		go func(i int, row []byte) {
			defer wg.Done()

			values := bytes.Split(row, []byte(","))

			ts, err := time.Parse("02/01/2006 15:04:05", string(values[2]))
			if err != nil {
				log.Println("could not parse time... skipping", string(values[2]))
			}
			// Build the request body.
			var b strings.Builder
			b.WriteString(`{"timestamp" : "`)
			b.WriteString(string(ts.Format(time.RFC3339)))
			b.WriteString(`",`)
			b.WriteString(`"dstport" : "`)
			b.WriteString(string(values[0]))
			b.WriteString(`",`)
			b.WriteString(`"protocol" : "`)
			b.WriteString(string(values[1]))
			b.WriteString(`"}`)

			// Set up the request object.
			req := esapi.IndexRequest{
				Index:      "netdata",
				DocumentID: strconv.Itoa(i + 1),
				Body:       strings.NewReader(b.String()),
				Refresh:    "true",
			}

			// Perform the request with the client.
			res, err := req.Do(context.Background(), ec)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("[%s] Error indexing document ID=%d", res.Status(), i+1)
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
				} else {
					// Print the response status and indexed document version.
					log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
				}
			}
		}(i, row)
	}
	wg.Wait()

	log.Println(strings.Repeat("-", 37))

	// 3. Search for the indexed documents
	//
	// Build the request body.
	var buf bytes.Buffer
	query := map[string]interface{}{
		"query": map[string]interface{}{
			"match": map[string]interface{}{
				"dstport": "443",
			},
		},
	}
	if err := json.NewEncoder(&buf).Encode(query); err != nil {
		log.Fatalf("Error encoding query: %s", err)
	}

	// Perform the search request.
	res, err = ec.Search(
		ec.Search.WithContext(context.Background()),
		ec.Search.WithIndex("test"),
		ec.Search.WithBody(&buf),
		ec.Search.WithTrackTotalHits(true),
		ec.Search.WithPretty(),
	)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			log.Fatalf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and error information.
			log.Fatalf("[%s] %s: %s",
				res.Status(),
				e["error"].(map[string]interface{})["type"],
				e["error"].(map[string]interface{})["reason"],
			)
		}
	}

	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print the response status, number of results, and request duration.
	log.Printf(
		"[%s] %d hits; took: %dms",
		res.Status(),
		int(r["hits"].(map[string]interface{})["total"].(map[string]interface{})["value"].(float64)),
		int(r["took"].(float64)),
	)
	// Print the ID and document source for each hit.
	for _, hit := range r["hits"].(map[string]interface{})["hits"].([]interface{}) {
		log.Printf(" * ID=%s, %s", hit.(map[string]interface{})["_id"], hit.(map[string]interface{})["_source"])
	}

	log.Println(strings.Repeat("=", 37))

}

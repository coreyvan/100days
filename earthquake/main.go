package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/elastic/go-elasticsearch/esapi"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/pkg/errors"
)

// Earthquake .
type Earthquake struct {
	ID        int     `json:"id"`
	TS        string  `json:"@timestamp"`
	Geo       Geo     `json:"geo"`
	Type      string  `json:"type"`
	Depth     float64 `json:"depth"`
	Magnitude float64 `json:"magnitude"`
	MagType   string  `json:"mag_type"`
	Source    string  `json:"source"`
	SourceID  string  `json:"source_id"`
}

// Geo .
type Geo struct {
	Lat  string `json:"lat"`
	Long string `json:"lon"`
}

// Batch .
type Batch struct {
	Payload []Earthquake
	ID      int
}

func main() {
	var (
		numRecords = 0
	)
	numUploaders, err := strconv.Atoi(os.Args[1])
	if err != nil {
		log.Fatal("Could not parse commandline argument:", err)
	}

	batchSize, err := strconv.Atoi(os.Args[2])
	if err != nil {
		log.Fatal("Could not parse commandline argument:", err)
	}

	file, err := os.Open("database.csv")
	if err != nil {
		log.Fatal("could not open file", err)
	}

	earthquakes, err := readEarthquakes(file, numRecords)
	if err != nil {
		log.Fatal("could not parse file", err)
	}

	log.Printf("Parsed %d records", len(*earthquakes))
	log.Println(strings.Repeat("-", 30))

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

	var r map[string]interface{}
	// Deserialize the response into a map.
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		log.Fatalf("Error parsing the response body: %s", err)
	}
	// Print client and server version numbers.
	log.Printf("ES Client: %s", elasticsearch.Version)
	log.Printf("ES Server: %s", r["version"].(map[string]interface{})["number"])
	log.Println(strings.Repeat("-", 30))

	q := make(chan Batch)
	done := make(chan bool)

	// Initialize workers
	for i := 0; i < numUploaders; i++ {
		go bulkUploader(q, i, ec, done)
	}

	// Queue jobs to workers
	defer timeTaken(time.Now(), numUploaders)
	numBatches := len(*earthquakes) / batchSize
	var payload []Earthquake
	var currBatch = 1

	for i, e := range *earthquakes {
		payload = append(payload, e)
		if (i+1)%batchSize == 0 || i+1 == len(*earthquakes) {
			log.Printf("Sending batch %d to queue", currBatch)
			go func(b Batch) {
				q <- b
			}(Batch{ID: currBatch, Payload: payload})
			currBatch++
			payload = nil
		}
	}

	// Stop when all jobs have been complete
	for c := 0; c < (numBatches); c++ {
		<-done
	}

}

func readEarthquakes(f *os.File, n int) (*[]Earthquake, error) {
	var earthquakes []Earthquake

	r := csv.NewReader(f)

	records, err := r.ReadAll()
	if err != nil {
		return nil, errors.Wrap(err, "could not read records")
	}

	log.Println(strings.Repeat("-", 30))
	log.Printf("Read %d records from database.csv", len(records))
	log.Println(strings.Repeat("-", 30))

	var i = 1
	for _, record := range records {
		if i > n && n != 0 {
			break
		}
		if len(record) < 21 {
			log.Println("Not enough required fields ...skipping")
			continue
		}
		tstring := record[0] + " " + record[1]
		ts, err := time.Parse("01/02/2006 15:04:05", tstring)
		if err != nil {
			log.Println("Invalid time format:", tstring, "...skipping")
			continue
		}
		geo := Geo{Lat: record[2], Long: record[3]}

		depth, err := strconv.ParseFloat(record[5], 64)
		if err != nil {
			log.Println("Invalid depth", err, "...skipping")
			continue
		}

		magnitude, err := strconv.ParseFloat(record[8], 64)
		if err != nil {
			log.Println("Invalid magnitude", err, "...skipping")
			continue
		}

		es := Earthquake{
			ID:        i,
			TS:        ts.Format("2006-01-02 15:04:05"),
			Geo:       geo,
			Type:      record[4],
			Depth:     depth,
			Magnitude: magnitude,
			MagType:   record[9],
			Source:    record[18],
			SourceID:  record[16],
		}
		earthquakes = append(earthquakes, es)
		i++
	}

	return &earthquakes, nil
}

func uploader(queue chan Earthquake, workernumber int, es *elasticsearch.Client, done chan bool) {
	for true {
		select {
		case e := <-queue:
			ej, err := json.Marshal(e)
			if err != nil {
				log.Printf("Worker %d: could not marshal json: %v ... skipping", workernumber, err)
				done <- true
				continue
			}
			req := esapi.IndexRequest{
				Index:      "earthquakes",
				DocumentID: strconv.Itoa(e.ID),
				Body:       bytes.NewReader(ej),
				Refresh:    "true",
			}

			res, err := req.Do(context.Background(), es)
			if err != nil {
				log.Fatalf("Error getting response: %s", err)
				done <- true
			}
			defer res.Body.Close()

			if res.IsError() {
				log.Printf("[%s] Error indexing document ID=%d", res.Status(), e.ID)
				done <- true
			} else {
				// Deserialize the response into a map.
				var r map[string]interface{}
				if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
					log.Printf("Error parsing the response body: %s", err)
					done <- true
				} else {
					// Print the response status and indexed document version.
					// log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
					done <- true
				}
			}
		}
	}
}

func bulkUploader(queue chan Batch, wnum int, es *elasticsearch.Client, done chan bool) {
	var (
		buf bytes.Buffer
		raw map[string]interface{}
	)

	for true {
		select {
		case batch := <-queue:
			for _, e := range batch.Payload {
				meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%d" } }%s`, e.ID, "\n"))
				data, err := json.Marshal(e)
				if err != nil {
					log.Printf("Worker %d: could not marshal json: %v ... skipping", wnum, err)
					continue
				}
				data = append(data, "\n"...)

				buf.Grow(len(meta) + len(data))
				buf.Write(meta)
				buf.Write(data)
			}

			// log.Printf("Worker %d: Writing batch %d\n", wnum, batch.ID) // uncomment to print progress
			res, err := es.Bulk(bytes.NewReader(buf.Bytes()), es.Bulk.WithIndex("earthquake_bulk"))
			if err != nil {
				log.Printf("Failure indexing batch %d: %s", batch.ID, err)
				done <- true
				continue
			}
			defer res.Body.Close()

			if res.IsError() {
				if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
					log.Printf("Failure to to parse response body: %s", err)
					done <- true
					continue
				} else {
					log.Printf("  Error: [%d] %s: %s",
						res.StatusCode,
						raw["error"].(map[string]interface{})["type"],
						raw["error"].(map[string]interface{})["reason"],
					)
					done <- true
					continue
				}
			}
			buf.Reset()
			done <- true
		}
	}
}

func timeTaken(t time.Time, n int) {
	elapsed := time.Since(t)
	log.Printf("Num uploaders: %d\t\t%s\n", n, elapsed)
}

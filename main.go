package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
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
	bytes := kbs * 1024
	fileName := os.Args[4]

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

	// downloader := s3manager.NewDownloader(sess)

	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal("could not open file", err)
	}
	defer file.Close()
	// svc := s3.New(sess)

	// objects, err := svc.ListObjects(&s3.ListObjectsInput{
	// 	Bucket: aws.String("cse-cic-ids2018"),
	// })
	// if err != nil {
	// 	log.Fatal("could not read objects", err)
	// }

	// for _, obj := range objects.Contents {
	// 	fmt.Println(obj.GoString())
	// }
	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.PartSize = 64 * 1024 * 1024 // 64MB per part
		d.Concurrency = 4
	})

	numBytes, err := downloader.Download(file, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		Range:  aws.String(fmt.Sprintf("bytes=0-%d", bytes)),
	})
	if err != nil {
		log.Fatal("could not download file", err)
	}

	fmt.Println("Downloaded", file.Name(), numBytes, "bytes")
}

package main

import (
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/pkg/errors"
)

// S3Client .
type S3Client struct {
	Session *session.Session
}

// Row .
type Row struct {
	DstPort  int
	Protocol int
	TS       time.Time
}

func (s *S3Client) getS3Data(w io.WriterAt, goi *s3.GetObjectInput, part int64) (int64, error) {
	downloader := s3manager.NewDownloader(s.Session, func(d *s3manager.Downloader) {
		d.PartSize = part * 1024 * 1024 // 64MB per part
		d.Concurrency = 4
	})

	numBytes, err := downloader.Download(w, goi)
	if err != nil {
		return 0, errors.Wrap(err, "could not download from S3")
	}
	return numBytes, nil
}

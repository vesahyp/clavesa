package preview

// fetchSourceResult is the preview-side counterpart to dataquery.readSource:
// supports local-file paths in addition to S3, walks back across recent
// Hive/bare-date partition prefixes when the named prefix is empty (sources
// produced by upstream pipelines may have no objects in today's partition
// yet), and stitches multiple objects together until the row limit is hit.
// The actual format parsing is delegated to dataquery's exported parsers
// (ParseCSV / ParseJSON / ParseParquet) so both packages stay in sync.

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/vesahyp/clavesa/internal/dataquery"
)

// looksLikeLocalPath reports whether s is a local filesystem path.
// S3 bucket names cannot start with /, ./, or ../.
func looksLikeLocalPath(s string) bool {
	return strings.HasPrefix(s, "/") || strings.HasPrefix(s, "./") || strings.HasPrefix(s, "../")
}

// fetchSourceResult fetches the first object at bucket/prefix (local or S3),
// parses up to limit rows in the given format, and returns a QueryResult.
// jsonPath is used when format == "json" to extract a nested array (e.g. "cars").
func fetchSourceResult(ctx context.Context, s3c dataquery.S3Client, bucket, prefix, format, jsonPath string, limit int) (*dataquery.QueryResult, error) {
	if looksLikeLocalPath(bucket) {
		f, err := os.Open(filepath.Join(bucket, prefix))
		if err != nil {
			return nil, err
		}
		defer f.Close()
		return parseByFormat(f, format, jsonPath, limit)
	}

	keys, err := findLatestS3Keys(ctx, s3c, bucket, prefix, 10)
	if err != nil {
		return nil, err
	}

	var combined *dataquery.QueryResult
	for _, key := range keys {
		getOut, err := s3c.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			return nil, fmt.Errorf("GetObject %s: %w", key, err)
		}
		qr, err := parseByFormat(getOut.Body, format, jsonPath, limit)
		getOut.Body.Close()
		if err != nil {
			return nil, err
		}
		if combined == nil {
			combined = qr
		} else {
			combined.Rows = append(combined.Rows, qr.Rows...)
			combined.RowCount = len(combined.Rows)
		}
		if combined.RowCount >= limit {
			combined.Rows = combined.Rows[:limit]
			combined.RowCount = limit
			combined.Truncated = true
			break
		}
	}
	if combined == nil {
		return nil, fmt.Errorf("no data found at s3://%s/%s", bucket, prefix)
	}
	return combined, nil
}

// parseByFormat dispatches to the right dataquery parser. Centralizes the
// switch so the local-file and S3 paths above can't drift on supported
// formats.
func parseByFormat(r io.Reader, format, jsonPath string, limit int) (*dataquery.QueryResult, error) {
	switch format {
	case "csv":
		return dataquery.ParseCSV(r, limit)
	case "json":
		return dataquery.ParseJSON(r, jsonPath, limit)
	case "parquet":
		return dataquery.ParseParquet(r, limit)
	default:
		return nil, fmt.Errorf("unsupported format: %s", format)
	}
}

// findLatestS3Keys returns up to maxKeys S3 keys sorted newest-first.
func findLatestS3Keys(ctx context.Context, s3c dataquery.S3Client, bucket, prefix string, maxKeys int) ([]string, error) {
	first, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("ListObjectsV2: %w", err)
	}
	if len(first.Contents) == 0 {
		return nil, fmt.Errorf("no objects found at s3://%s/%s", bucket, prefix)
	}

	firstKey := aws.ToString(first.Contents[0].Key)
	segments := strings.Split(firstKey, "/")

	var staticPrefix, dateFormat string
	for i, seg := range segments {
		if strings.HasPrefix(seg, "year=") {
			staticPrefix = strings.Join(segments[:i], "/") + "/"
			dateFormat = "hive"
			break
		}
		if len(seg) == 4 && seg >= "2000" && seg <= "2099" {
			staticPrefix = strings.Join(segments[:i], "/") + "/"
			dateFormat = "bare"
			break
		}
	}

	var objects []s3types.Object
	if staticPrefix != "" && dateFormat != "" {
		now := time.Now().UTC()
		var probes []string
		switch dateFormat {
		case "hive":
			for _, offset := range []int{0, 1, 3} {
				d := now.AddDate(0, 0, -offset)
				probes = append(probes, fmt.Sprintf("%syear=%d/month=%02d/day=%02d/",
					staticPrefix, d.Year(), d.Month(), d.Day()))
			}
			probes = append(probes, fmt.Sprintf("%syear=%d/month=%02d/",
				staticPrefix, now.Year(), now.Month()))
		case "bare":
			for _, offset := range []int{0, 1, 3} {
				d := now.AddDate(0, 0, -offset)
				probes = append(probes, fmt.Sprintf("%s%d/%02d/%02d/",
					staticPrefix, d.Year(), d.Month(), d.Day()))
			}
			probes = append(probes, fmt.Sprintf("%s%d/%02d/",
				staticPrefix, now.Year(), now.Month()))
		}
		for _, probe := range probes {
			out, listErr := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket:  aws.String(bucket),
				Prefix:  aws.String(probe),
				MaxKeys: aws.Int32(1000),
			})
			if listErr != nil {
				return nil, fmt.Errorf("ListObjectsV2: %w", listErr)
			}
			if len(out.Contents) > 0 {
				objects = out.Contents
				break
			}
		}
	}

	if len(objects) == 0 {
		objects = first.Contents
	}

	// Sort newest first.
	sort.Slice(objects, func(i, j int) bool {
		ti := objects[i].LastModified
		tj := objects[j].LastModified
		if ti == nil || tj == nil {
			return false
		}
		return ti.After(*tj)
	})

	keys := make([]string, 0, maxKeys)
	for i, obj := range objects {
		if i >= maxKeys {
			break
		}
		keys = append(keys, aws.ToString(obj.Key))
	}
	return keys, nil
}

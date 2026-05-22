package dataquery

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	parquetgo "github.com/parquet-go/parquet-go"

	"github.com/vesahyp/clavesa/internal/graph"
)

// S3Client is the subset of the AWS S3 API used by the source handler.
// Defining an interface allows tests to inject a mock without depending on the
// real SDK client.
type S3Client interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
}

// readSource fetches the first object matching the given bucket/prefix, parses
// up to limit rows according to format, and returns a QueryResult.
// jsonPath is only used when format == "json"; see parseJSON for details.
func readSource(ctx context.Context, s3c S3Client, bucket, prefix, format, jsonPath string, limit int) (*QueryResult, error) {
	// Find the first object under the prefix.
	listOut, err := s3c.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
		Prefix:  aws.String(prefix),
		MaxKeys: aws.Int32(1),
	})
	if err != nil {
		return nil, fmt.Errorf("ListObjectsV2: %w", err)
	}
	if len(listOut.Contents) == 0 {
		return nil, errNotFound("no objects found at s3://" + bucket + "/" + prefix)
	}

	key := aws.ToString(listOut.Contents[0].Key)
	getOut, err := s3c.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("GetObject: %w", err)
	}
	defer getOut.Body.Close()

	switch format {
	case "csv":
		return ParseCSV(getOut.Body, limit)
	case "json":
		return ParseJSON(getOut.Body, jsonPath, limit)
	case "parquet":
		return ParseParquet(getOut.Body, limit)
	default:
		return nil, errBadRequest("unsupported format: " + format)
	}
}

// ParseCSV reads up to limit data rows (excluding the header) from r.
// Exported so the preview package can share the same parser; the source
// previewer hits both local files and S3 objects but parses the same way.
func ParseCSV(r io.Reader, limit int) (*QueryResult, error) {
	cr := csv.NewReader(r)
	header, err := cr.Read()
	if err != nil {
		return nil, fmt.Errorf("read CSV header: %w", err)
	}

	cols := make([]graph.Column, len(header))
	for i, h := range header {
		cols[i] = graph.Column{Name: h, Type: "string", Nullable: true}
	}

	var rows [][]string
	truncated := false
	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read CSV row: %w", err)
		}
		if len(rows) >= limit {
			truncated = true
			break
		}
		rows = append(rows, rec)
	}

	return &QueryResult{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
	}, nil
}

// ParseJSON reads up to limit records from r. If jsonPath is empty, r is
// treated as newline-delimited JSON (NDJSON). If jsonPath is non-empty (e.g.
// "cars" or "data.items"), r is parsed as a single JSON document and the value
// at the dot-separated path is extracted as an array of records. Shared with
// the preview package — see ParseCSV.
func ParseJSON(r io.Reader, jsonPath string, limit int) (*QueryResult, error) {
	if jsonPath == "" {
		return parseNDJSON(r, limit)
	}
	return parseJSONArray(r, jsonPath, limit)
}

// parseJSONArray reads a full JSON document from r, navigates to the value at
// the dot-separated jsonPath, and iterates it as an array of objects.
func parseJSONArray(r io.Reader, jsonPath string, limit int) (*QueryResult, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read JSON body: %w", err)
	}
	var root interface{}
	if err := json.Unmarshal(data, &root); err != nil {
		return nil, fmt.Errorf("parse JSON: %w", err)
	}
	cur := root
	for _, part := range strings.Split(jsonPath, ".") {
		if part == "" {
			continue
		}
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("json_path %q: expected object at key %q", jsonPath, part)
		}
		cur, ok = m[part]
		if !ok {
			return nil, fmt.Errorf("json_path %q: key %q not found", jsonPath, part)
		}
	}
	arr, ok := cur.([]interface{})
	if !ok {
		return nil, fmt.Errorf("json_path %q: expected array, got %T", jsonPath, cur)
	}
	return rowsFromMaps(jsonArrayToMaps(arr, limit))
}

// jsonArrayToMaps converts a []interface{} of JSON objects into
// ([]map[string]interface{}, truncated). Non-object elements are skipped.
func jsonArrayToMaps(arr []interface{}, limit int) ([]map[string]interface{}, bool) {
	var rows []map[string]interface{}
	truncated := false
	for _, item := range arr {
		if len(rows) >= limit {
			truncated = true
			break
		}
		obj, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		rows = append(rows, obj)
	}
	return rows, truncated
}

// rowsFromMaps converts a slice of JSON objects into a QueryResult, sorting
// column names for determinism. All values are converted to strings.
func rowsFromMaps(rawRows []map[string]interface{}, truncated bool) (*QueryResult, error) {
	colSet := make(map[string]struct{})
	for _, obj := range rawRows {
		for k := range obj {
			colSet[k] = struct{}{}
		}
	}
	colNames := make([]string, 0, len(colSet))
	for k := range colSet {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)
	cols := make([]graph.Column, len(colNames))
	for i, n := range colNames {
		cols[i] = graph.Column{Name: n, Type: "string", Nullable: true}
	}
	rows := make([][]string, len(rawRows))
	for i, obj := range rawRows {
		row := make([]string, len(colNames))
		for j, col := range colNames {
			v := obj[col]
			if v == nil {
				row[j] = ""
			} else {
				row[j] = fmt.Sprintf("%v", v)
			}
		}
		rows[i] = row
	}
	return &QueryResult{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
	}, nil
}

// parseNDJSON reads up to limit rows from a newline-delimited JSON stream.
// Columns are discovered from the union of keys in the first seen object; key
// order is sorted for determinism. All values are converted to strings.
func parseNDJSON(r io.Reader, limit int) (*QueryResult, error) {
	scanner := bufio.NewScanner(r)
	// Increase scanner buffer to handle large JSON lines.
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	// First pass: collect all rows as raw maps to discover the column set.
	var rawRows []map[string]interface{}
	colSet := make(map[string]struct{})

	truncated := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		if len(rawRows) >= limit {
			truncated = true
			break
		}
		var obj map[string]interface{}
		if err := json.Unmarshal(line, &obj); err != nil {
			return nil, fmt.Errorf("parse NDJSON line: %w", err)
		}
		for k := range obj {
			colSet[k] = struct{}{}
		}
		rawRows = append(rawRows, obj)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan NDJSON: %w", err)
	}

	// Sort column names for a stable order.
	colNames := make([]string, 0, len(colSet))
	for k := range colSet {
		colNames = append(colNames, k)
	}
	sort.Strings(colNames)

	cols := make([]graph.Column, len(colNames))
	for i, n := range colNames {
		cols[i] = graph.Column{Name: n, Type: "string", Nullable: true}
	}

	rows := make([][]string, len(rawRows))
	for i, obj := range rawRows {
		row := make([]string, len(colNames))
		for j, col := range colNames {
			v := obj[col]
			if v == nil {
				row[j] = ""
			} else {
				row[j] = fmt.Sprintf("%v", v)
			}
		}
		rows[i] = row
	}

	return &QueryResult{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
	}, nil
}

// parseParquet reads up to limit rows from a Parquet-formatted byte stream.
// Column types are reported as "string". It uses the low-level parquet.File
// API to avoid the generic reader's restriction that the type argument must be
// a concrete struct (not map[string]interface{}).
func ParseParquet(r io.Reader, limit int) (*QueryResult, error) {
	// parquet-go requires a ReaderAt; read the full body into memory first.
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("read parquet body: %w", err)
	}

	pf, err := parquetgo.OpenFile(newBytesReaderAt(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open parquet file: %w", err)
	}

	schema := pf.Schema()
	fields := schema.Fields()

	cols := make([]graph.Column, len(fields))
	for i, f := range fields {
		cols[i] = graph.Column{Name: f.Name(), Type: "string", Nullable: true}
	}

	// Collect rows by iterating over row groups.
	var rows [][]string
	truncated := false

	for _, rg := range pf.RowGroups() {
		if truncated {
			break
		}
		rgReader := parquetgo.NewRowGroupReader(rg)
		rowBuf := make([]parquetgo.Row, 64)
		for {
			n, readErr := rgReader.ReadRows(rowBuf)
			for i := 0; i < n; i++ {
				if len(rows) >= limit {
					truncated = true
					break
				}
				row := make([]string, len(fields))
				for j := range fields {
					// Each parquet.Row is a flat []Value slice ordered by column.
					if j < len(rowBuf[i]) {
						row[j] = fmt.Sprintf("%v", rowBuf[i][j])
					}
				}
				rows = append(rows, row)
			}
			if truncated || readErr == io.EOF || n == 0 {
				break
			}
			if readErr != nil {
				return nil, fmt.Errorf("read parquet row group: %w", readErr)
			}
		}
	}

	return &QueryResult{
		Columns:   cols,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
	}, nil
}

// WriteParquetRows is an exported helper used by tests to produce valid Parquet
// bytes in memory. v must be a slice of a struct type annotated with `parquet`
// field tags.
func WriteParquetRows[T any](w io.Writer, rows []T) error {
	pw := parquetgo.NewGenericWriter[T](w)
	if _, err := pw.Write(rows); err != nil {
		return err
	}
	return pw.Close()
}

// bytesReaderAt wraps a []byte so it satisfies io.ReaderAt (required by
// parquet-go's OpenFile).
type bytesReaderAt struct {
	data []byte
}

func newBytesReaderAt(data []byte) *bytesReaderAt {
	return &bytesReaderAt{data: data}
}

func (b *bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= int64(len(b.data)) {
		return 0, io.EOF
	}
	n := copy(p, b.data[off:])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}

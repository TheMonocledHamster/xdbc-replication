package dataset

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"path"
)

// Fetch streams the selected member as CSV. Native Lineitem pipe records are
// converted without discarding commas in comments. Downloads are explicit.
func (d Dataset) Fetch(ctx context.Context, output io.Writer) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, d.ArchiveURL, nil)
	if err != nil {
		return err
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("dataset download: HTTP %d", response.StatusCode)
	}
	gzipReader, err := gzip.NewReader(response.Body)
	if err != nil {
		return fmt.Errorf("dataset archive: %w", err)
	}
	defer gzipReader.Close()
	archive := tar.NewReader(gzipReader)
	for {
		header, err := archive.Next()
		if err == io.EOF {
			return fmt.Errorf("%s is absent from dataset archive", d.ArchiveMember)
		}
		if err != nil {
			return err
		}
		if header.Typeflag == tar.TypeReg && path.Base(header.Name) == d.ArchiveMember {
			if err := d.writeCSV(archive, output); err != nil {
				return err
			}
			// Drain to validate gzip's checksum, not just the selected member's bytes.
			if _, err := io.Copy(io.Discard, gzipReader); err != nil {
				return err
			}
			return nil
		}
	}
}

// writeCSV preserves ordinary CSV bytes, including quoted empty strings. Only
// pipe records need normalization: optionally remove a final empty field and
// let csv.Writer quote any commas in comments.
func (d Dataset) writeCSV(input io.Reader, output io.Writer) error {
	if d.ArchiveDelimiter == "" {
		_, err := io.Copy(output, input)
		return err
	}
	delimiter := []rune(d.ArchiveDelimiter)
	if len(delimiter) != 1 {
		return fmt.Errorf("invalid archive delimiter")
	}
	reader := csv.NewReader(input)
	reader.Comma = delimiter[0]
	reader.FieldsPerRecord = -1
	writer := csv.NewWriter(output)
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if d.ArchiveTrailingField {
			if len(record) != len(d.Config.Fields)+1 || record[len(record)-1] != "" {
				return fmt.Errorf("invalid trailing field in %s record", d.Name)
			}
			record = record[:len(record)-1]
		}
		if len(record) != len(d.Config.Fields) {
			return fmt.Errorf("%s record has %d fields, expected %d", d.Name, len(record), len(d.Config.Fields))
		}
		if err := writer.Write(record); err != nil {
			return err
		}
	}
	writer.Flush()
	return writer.Error()
}

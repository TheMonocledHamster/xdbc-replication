package dataset

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
)

func TestPaperFixtures(t *testing.T) {
	t.Setenv("COMPRESSION", "")
	for name, columns := range map[string]int{"lineitem": 16, "acs": 230, "iot": 85, "icu": 26} {
		t.Run(name, func(t *testing.T) {
			d, err := Load(filepath.Join("../../testdata", name, "transfer.json"))
			if err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(filepath.Join(d.Directory, "sample.csv"))
			if err != nil {
				t.Fatal(err)
			}
			if got := fmt.Sprintf("%x", sha256.Sum256(data)); got != d.SampleSHA256 {
				t.Fatal("sample checksum changed")
			}
			records, err := csv.NewReader(bytes.NewReader(data)).ReadAll()
			if err != nil {
				t.Fatal(err)
			}
			if len(d.Config.Fields) != columns || int64(len(records)-1) != d.SampleRows {
				t.Fatalf("wrong fixture dimensions: %d columns, %d rows", len(d.Config.Fields), len(records)-1)
			}
			for i, field := range d.Config.Fields {
				if records[0][i] != field.Name {
					t.Fatalf("header %d does not match configured column", i)
				}
				if name == "lineitem" {
					if i == 0 && field.Type != row.Int64 {
						t.Fatal("order key must match PostgreSQL bigint")
					}
					if i >= 4 && i <= 7 && field.Type != row.Numeric {
						t.Fatal("decimal fields must remain exact numeric")
					}
					if i >= 10 && i <= 12 && field.Type != row.Date {
						t.Fatal("date fields must retain PostgreSQL DATE")
					}
				}
			}
		})
	}
}

func TestNativeLineitemConversionPreservesComment(t *testing.T) {
	d, err := Load("../../testdata/lineitem/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	input := "1|636998|36999|3|8|15479.68|0.10|0.02|N|O|1996-01-29|1996-03-05|1996-01-31|TAKE BACK RETURN|REG AIR|riously. regular, express dep\n"
	var output bytes.Buffer
	if err := d.writeCSV(strings.NewReader(input), &output); err != nil {
		t.Fatal(err)
	}
	record, err := csv.NewReader(&output).Read()
	if err != nil || len(record) != 16 || record[15] != "riously. regular, express dep" {
		t.Fatalf("archive record changed: %q, %v", record, err)
	}
}

func TestTrailingPipeValidation(t *testing.T) {
	d := Dataset{Name: "lineitem", ArchiveDelimiter: "|", ArchiveTrailingField: true}
	d.Config.Fields = make([]row.Field, 3)
	var output bytes.Buffer
	if err := d.writeCSV(strings.NewReader("1|2|hello, world|\n"), &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "1,2,\"hello, world\"\n" {
		t.Fatalf("comment changed: %q", output.String())
	}
	if err := d.writeCSV(strings.NewReader("1|2|comment|unexpected\n"), &bytes.Buffer{}); err == nil {
		t.Fatal("silently discarded a non-empty field")
	}
}

func TestFetchValidatesArchiveAndPreservesCSV(t *testing.T) {
	var archive bytes.Buffer
	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)
	data := []byte("1,\"\"\n2,\n")
	if err := tw.WriteHeader(&tar.Header{Name: "nested/values.csv", Mode: 0600, Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	tw.Write(data)
	tw.Close()
	gz.Close()
	for _, broken := range []bool{false, true} {
		t.Run(fmt.Sprintf("broken=%t", broken), func(t *testing.T) {
			body := archive.Bytes()
			if broken {
				body = body[:len(body)-8]
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(body) }))
			defer server.Close()
			d := Dataset{ArchiveURL: server.URL, ArchiveMember: "values.csv"}
			var output bytes.Buffer
			err := d.Fetch(context.Background(), &output)
			if broken {
				if err == nil {
					t.Fatal("accepted truncated gzip archive")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(data, output.Bytes()) {
				t.Fatal("changed quoted empty text into NULL")
			}
		})
	}
}

// Package dataset describes the paper's four PostgreSQL datasets and fixtures.
package dataset

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/TheMonocledHamster/xdbc-replication/internal/transfer"
)

type Dataset struct {
	ArchiveDelimiter     string          `json:"archive_delimiter"`
	ArchiveTrailingField bool            `json:"archive_trailing_field"`
	Name                 string          `json:"name"`
	ArchiveURL           string          `json:"archive_url"`
	ArchiveMember        string          `json:"archive_member"`
	FullHeader           bool            `json:"full_header"`
	SampleRows           int64           `json:"sample_rows"`
	SampleSHA256         string          `json:"sample_sha256"`
	ArtifactRevision     string          `json:"artifact_revision"`
	Config               transfer.Config `json:"-"`
	Directory            string          `json:"-"`
}

func Load(configPath string) (Dataset, error) {
	var d Dataset
	c, err := transfer.LoadConfig(configPath)
	if err != nil {
		return d, err
	}
	d.Directory = filepath.Dir(configPath)
	b, err := os.ReadFile(filepath.Join(d.Directory, "dataset.json"))
	if err != nil {
		return d, err
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return d, err
	}
	if d.Name == "" || d.SampleRows <= 0 || d.ArchiveURL == "" || d.ArchiveMember == "" {
		return d, fmt.Errorf("incomplete dataset metadata in %s", d.Directory)
	}
	d.Config = c
	return d, nil
}

// OrderedQuery orders complete rows, preserving duplicates without assuming a
// unique key. PostgreSQL performs the sort; Go never holds the full relation.
func (d Dataset) OrderedQuery() string {
	positions := make([]string, len(d.Config.Fields))
	for i := range positions {
		positions[i] = strconv.Itoa(i + 1)
	}
	return d.Config.Query() + " ORDER BY " + strings.Join(positions, ", ")
}

func (d Dataset) CopyQuery() string {
	return "COPY (" + d.OrderedQuery() + ") TO STDOUT WITH (FORMAT csv, HEADER true)"
}

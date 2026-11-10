// Package transfer connects the existing XDBC stages into one-transfer runtimes.
package transfer

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
	"github.com/TheMonocledHamster/xdbc-replication/internal/network"
	"github.com/jackc/pgx/v5"
)

type Config struct {
	Schema        string      `json:"schema"`
	Table         string      `json:"table"`
	Compression   string      `json:"compression"`
	BufferSize    int         `json:"buffer_size"`
	PoolSize      int         `json:"pool_size"`
	QueueCapacity int         `json:"queue_capacity"`
	Fields        []row.Field `json:"fields"`
}

func LoadConfig(path string) (Config, error) {
	var c Config
	b, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	if err := json.Unmarshal(b, &c); err != nil {
		return c, err
	}
	// An environment override lets Compose exercise both modes with the same file.
	if mode := os.Getenv("COMPRESSION"); mode != "" {
		c.Compression = mode
	}
	return c, c.Validate()
}

func (c Config) Validate() error {
	if c.Schema == "" || c.Table == "" {
		return fmt.Errorf("schema and table are required")
	}
	l, err := row.NewLayout(c.Fields)
	if err != nil {
		return err
	}
	if c.BufferSize < l.RowSize() {
		return fmt.Errorf("buffer_size must fit at least one row (%d bytes)", l.RowSize())
	}
	if c.QueueCapacity < 1 || c.PoolSize < 2*c.QueueCapacity+4 {
		return fmt.Errorf("queue_capacity must be positive and pool_size must be at least 2*queue_capacity+4 (one worker per stage)")
	}
	if c.Compression != "none" && c.Compression != "snappy" {
		return fmt.Errorf("compression must be none or snappy")
	}
	return nil
}

func (c Config) Relation() string { return pgx.Identifier{c.Schema, c.Table}.Sanitize() }
func (c Config) Columns() []string {
	names := make([]string, len(c.Fields))
	for i, f := range c.Fields {
		names[i] = f.Name
	}
	return names
}
func (c Config) Query() string {
	names := c.Columns()
	for i := range names {
		names[i] = pgx.Identifier{names[i]}.Sanitize()
	}
	return "SELECT " + strings.Join(names, ", ") + " FROM " + c.Relation()
}
func (c Config) Network() network.Config {
	mode := network.CompressionNone
	if c.Compression == "snappy" {
		mode = network.CompressionSnappy
	}
	return network.Config{PayloadCap: c.BufferSize, Compression: mode, Format: network.FormatRow}
}

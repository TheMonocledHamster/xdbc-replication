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
	Workers       Workers     `json:"workers"`
	Fields        []row.Field `json:"fields"`
}

type Workers struct {
	Read       int `json:"read"`
	Compress   int `json:"compress"`
	Network    int `json:"network"`
	Decompress int `json:"decompress"`
	Write      int `json:"write"`
}

// Replaces unset counts with one if not specified
func (w Workers) withDefaults() Workers {
	for _, n := range []*int{&w.Read, &w.Compress, &w.Network, &w.Decompress, &w.Write} {
		if *n == 0 {
			*n = 1
		}
	}
	return w
}

func (w Workers) validate() error {
	for name, n := range map[string]int{"read": w.Read, "compress": w.Compress, "network": w.Network,
		"decompress": w.Decompress, "write": w.Write} {
		if n < 0 {
			return fmt.Errorf("workers.%s must not be negative", name)
		}
	}
	return nil
}

// Stages returns the effective worker counts, with defaults applied.
func (c Config) Stages() Workers { return c.Workers.withDefaults() }

// MinPool is the smallest pool that cannot deadlock
// Could do distinct mins for server and client, but only sacrifices a little bit of memory
func (c Config) MinPool() int {
	w := c.Stages()
	server := w.Read + 2*w.Compress + w.Network
	client := w.Network + 2*w.Decompress + w.Write
	return 2*c.QueueCapacity + max(server, client)
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
	if err := c.Workers.validate(); err != nil {
		return err
	}
	if c.QueueCapacity < 1 || c.PoolSize < c.MinPool() {
		return fmt.Errorf("queue_capacity must be positive and pool_size must be at least %d "+
			"(2*queue_capacity plus the buffers held by the configured workers)", c.MinPool())
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

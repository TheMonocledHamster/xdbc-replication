package target

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/TheMonocledHamster/xdbc-replication/internal/format/row"
)

// Synthetic non-NULL rows with real dataset layouts isolate decoding from
// database, network, and compression costs.
func BenchmarkDecodeRow(b *testing.B) {
	for _, name := range []string{"acs", "lineitem"} {
		b.Run(name, func(b *testing.B) {
			data, err := os.ReadFile("../../testdata/" + name + "/transfer.json")
			if err != nil {
				b.Fatal(err)
			}
			var cfg struct {
				Fields []row.Field `json:"fields"`
			}
			if err := json.Unmarshal(data, &cfg); err != nil {
				b.Fatal(err)
			}
			layout, err := row.NewLayout(cfg.Fields)
			if err != nil {
				b.Fatal(err)
			}
			encoded := make([]byte, layout.RowSize())
			for i, field := range cfg.Fields {
				switch field.Type {
				case row.Numeric:
					err = layout.PutNumeric(encoded, i, "123.00")
				case row.String:
					err = layout.PutString(encoded, i, "value")
				case row.Char:
					err = layout.PutChar(encoded, i, 'A')
				case row.Int32:
					err = layout.PutInt32(encoded, i, 1000)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
			w := Writer{layout: layout, fields: cfg.Fields}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := w.decodeRow(encoded); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

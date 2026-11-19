package transfer

import (
	"reflect"
	"testing"
	"time"
)

func TestFixtureConfigAndPoolBudget(t *testing.T) {
	t.Setenv("COMPRESSION", "snappy")
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Fields) != 85 || c.Compression != "snappy" {
		t.Fatalf("unexpected fixture config: %+v", c)
	}
	if c.Fields[1].Name != "flowid" {
		t.Fatal("schema must use actual PostgreSQL column names")
	}
	c.PoolSize = 2*c.QueueCapacity + 3
	if c.Validate() == nil {
		t.Fatal("accepted pool that can deadlock compression")
	}
}

func TestWorkersDefaultToOneAndSizePool(t *testing.T) {
	t.Setenv("COMPRESSION", "snappy")
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := c.Stages(); got != (Workers{1, 1, 1, 1, 1}) {
		t.Fatalf("omitted workers must default to one each, got %+v", got)
	}
	if got, want := c.MinPool(), 2*c.QueueCapacity+4; got != want {
		t.Fatalf("single-worker MinPool = %d, want %d", got, want)
	}

	// Server needs 4+2*3+2 = 12 beyond the queues; client needs 2+2*2+5 = 11.
	c.Workers = Workers{Read: 4, Compress: 3, Network: 2, Decompress: 2, Write: 5}
	c.PoolSize = 2*c.QueueCapacity + 12
	if err := c.Validate(); err != nil {
		t.Fatalf("pool at the computed minimum rejected: %v", err)
	}
	c.PoolSize--
	if c.Validate() == nil {
		t.Fatal("accepted pool too small for the configured workers")
	}

	// The client side can be the larger requirement.
	c.Workers = Workers{Read: 1, Compress: 1, Network: 1, Decompress: 4, Write: 4}
	if got, want := c.MinPool(), 2*c.QueueCapacity+1+2*4+4; got != want {
		t.Fatalf("client-bound MinPool = %d, want %d", got, want)
	}

	c.Workers = Workers{Compress: -1}
	c.PoolSize = 100
	if c.Validate() == nil {
		t.Fatal("accepted negative worker count")
	}
}

// Each dataset's transfer_multi.json must stay a copy of its transfer.json that
// differs only in worker counts and the pool sized for them.
func TestMultiWorkerFixturesMatchSingleWorker(t *testing.T) {
	t.Setenv("COMPRESSION", "snappy")
	for _, name := range []string{"acs", "icu", "iot", "lineitem"} {
		t.Run(name, func(t *testing.T) {
			single, err := LoadConfig("../../testdata/" + name + "/transfer.json")
			if err != nil {
				t.Fatal(err)
			}
			multi, err := LoadConfig("../../testdata/" + name + "/transfer_multi.json")
			if err != nil {
				t.Fatal(err)
			}
			w := multi.Stages()
			if w.Read < 2 || w.Compress < 2 || w.Network < 2 || w.Decompress < 2 || w.Write < 2 {
				t.Fatalf("multi-worker fixture has a single-worker stage: %+v", w)
			}
			if multi.PoolSize != multi.MinPool() {
				t.Errorf("pool_size = %d; the fixture should sit at the minimum, %d", multi.PoolSize, multi.MinPool())
			}
			multi.Workers, multi.PoolSize = single.Workers, single.PoolSize
			if !reflect.DeepEqual(single, multi) {
				t.Fatalf("transfer_multi.json differs from transfer.json beyond workers and pool_size:\n%+v\n%+v", single, multi)
			}
		})
	}
}

func TestPartitionQueryCoversPagesOnce(t *testing.T) {
	t.Setenv("COMPRESSION", "snappy")
	c, err := LoadConfig("../../testdata/iot/transfer.json")
	if err != nil {
		t.Fatal(err)
	}
	if got := c.PartitionQuery(0, 1, 1000); got != c.Query() {
		t.Fatalf("a single reader must scan the whole table unfiltered, got %q", got)
	}
	// 10 pages over 3 readers: [0,3) [3,6) [6,∞) — adjacent, no gaps or overlap,
	// and the last range is open-ended.
	for part, want := range []string{
		" WHERE ctid >= '(0,0)' AND ctid < '(3,0)'",
		" WHERE ctid >= '(3,0)' AND ctid < '(6,0)'",
		" WHERE ctid >= '(6,0)'",
	} {
		if got := c.PartitionQuery(part, 3, 10); got != c.Query()+want {
			t.Errorf("part %d = %q, want suffix %q", part, got, want)
		}
	}
	// More readers than pages leaves some ranges empty but still valid, and the
	// last one still catches everything beyond.
	if got := c.PartitionQuery(1, 4, 2); got != c.Query()+" WHERE ctid >= '(0,0)' AND ctid < '(1,0)'" {
		t.Errorf("sparse range = %q", got)
	}
	if got := c.PartitionQuery(3, 4, 0); got != c.Query()+" WHERE ctid >= '(0,0)'" {
		t.Errorf("empty table, last reader = %q", got)
	}
}

func TestTimeout(t *testing.T) {
	for _, value := range []string{"", "30m", "0s", "-1s", "invalid"} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("TRANSFER_TIMEOUT", value)
			got, err := Timeout()
			valid := value == "" || value == "30m"
			if valid {
				want := time.Minute
				if value == "30m" {
					want = 30 * time.Minute
				}
				if err != nil || got != want {
					t.Fatalf("valid timeout rejected: %v", err)
				}
			} else if err == nil {
				t.Fatal("invalid timeout accepted")
			}
		})
	}
}

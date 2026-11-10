package transfer

import (
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

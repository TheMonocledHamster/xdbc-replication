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

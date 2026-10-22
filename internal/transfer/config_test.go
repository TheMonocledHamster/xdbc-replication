package transfer

import "testing"

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

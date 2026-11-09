package service

import (
	"context"
	"strings"
	"testing"
)

func TestParseCompareResponse(t *testing.T) {
	raw := []byte(`{"status":"ok","operation":"uc_compare","pass":false,"existing":"c.s.t","shadow":"c.s.t_clavesa",
	 "partition":"event_date","sums":["amount"],"where":"",
	 "partitions":[{"partition":"2026-11-01","status":"match","rows":[3,3],"sum_amount":["10.500000","10.500000"]},
	               {"partition":"2026-11-02","status":"missing in shadow","rows":[2,null],"sum_amount":["4.000000",null]}],
	 "schema_diff":[]}`)
	got, err := parseCompareResponse(raw)
	if err != nil {
		t.Fatal(err)
	}
	if got.Pass || len(got.Partitions) != 2 {
		t.Fatalf("got %+v", got)
	}
	p := got.Partitions[1]
	if p.Status != "missing in shadow" || *p.Rows[0] != 2 || p.Rows[1] != nil || p.Sums["amount"][0] != "4.000000" {
		t.Errorf("partition 2 = %+v", p)
	}
	if _, err := parseCompareResponse([]byte(`{"status":"error","message":"boom"}`)); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Errorf("error response: %v", err)
	}
}

func TestCompareDatabricksTablesValidates(t *testing.T) {
	svc := newDatabricksWorkspace(t, &stubDatabricks{})
	for _, in := range []DatabricksCompareInput{
		{Existing: "s.t", Shadow: "c.s.u", Partition: "d"},
		{Existing: "c.s.t", Shadow: "c.s.t", Partition: "d"},
		{Existing: "c.s.t", Shadow: "c.s.u"},
	} {
		if _, err := svc.CompareDatabricksTables(context.Background(), "p", in); err == nil {
			t.Errorf("%+v: want an error", in)
		}
	}
}

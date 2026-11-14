package service

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const exampleBundle = "../../testdata/bundles/daily_sales"

func TestCompareBundleJob(t *testing.T) {
	var asked []DatabricksCompareInput
	defer func(f func(*Service, context.Context, string, DatabricksCompareInput) (DatabricksCompareResult, error)) {
		compareTables = f
	}(compareTables)
	compareTables = func(_ *Service, _ context.Context, pipeline string, in DatabricksCompareInput) (DatabricksCompareResult, error) {
		if pipeline != "daily_sales_aws_daily_sales" {
			t.Errorf("pipeline %s", pipeline)
		}
		asked = append(asked, in)
		return DatabricksCompareResult{Pass: true, Existing: in.Existing, Shadow: in.Shadow,
			Partitions: []DatabricksComparePartition{{Partition: "2026-11-13", Status: "match"}}}, nil
	}
	s := &Service{workspace: t.TempDir()}
	res, err := s.CompareBundleJob(context.Background(), BundleOptions{Dir: exampleBundle, Target: "aws"}, "daily_sales", 7)
	if err != nil {
		t.Fatal(err)
	}
	if len(asked) != 1 || asked[0].Existing != "main.sales.orders_clean" || asked[0].Shadow != "main.sales.orders_clean_clavesa" ||
		asked[0].Partition != "order_date" || strings.Join(asked[0].Sums, ",") != "amount" ||
		asked[0].Where != "order_date >= date_sub(current_date(), 7)" {
		t.Errorf("compared %+v", asked)
	}
	if !res.Pass {
		t.Errorf("one table compared and passed, the others skipped: want PASS, got %+v", res)
	}
	skipped := 0
	for _, tb := range res.Tables {
		if tb.Skipped != "" {
			skipped++
		}
	}
	if skipped != 3 {
		t.Errorf("tables without clavesa.compare should be skipped (3): %+v", res.Tables)
	}

	compareTables = func(_ *Service, _ context.Context, _ string, in DatabricksCompareInput) (DatabricksCompareResult, error) {
		return DatabricksCompareResult{Pass: false}, nil
	}
	res, _ = s.CompareBundleJob(context.Background(), BundleOptions{Dir: exampleBundle, Target: "aws"}, "daily_sales", 0)
	if res.Pass {
		t.Error("a failing table must fail the job")
	}
	if _, err := s.CompareBundleJob(context.Background(), BundleOptions{Dir: exampleBundle, Target: "aws-local"}, "daily_sales", 0); err == nil ||
		!strings.Contains(err.Error(), "catalog: unity in shadow mode") {
		t.Errorf("aws-local is catalog: glue: %v", err)
	}
}

func TestBundleSwitchStepsGolden(t *testing.T) {
	got, err := BundleSwitchSteps(context.Background(), BundleOptions{Dir: exampleBundle, Target: "aws"}, "daily_sales", "prod")
	if err != nil {
		t.Fatal(err)
	}
	p := filepath.Join(exampleBundle, "expected", "switch-aws.txt")
	if os.Getenv("UPDATE_GOLDEN") == "1" {
		_ = os.WriteFile(p, []byte(got), 0o644)
	}
	want, _ := os.ReadFile(p)
	if got != string(want) {
		t.Errorf("switch steps differ from %s (UPDATE_GOLDEN=1 rewrites):\n%s", p, got)
	}
}

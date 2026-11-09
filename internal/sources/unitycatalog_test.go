package sources

import "testing"

func TestValidUCTable(t *testing.T) {
	t.Parallel()
	for _, ok := range []string{"main.sales.orders", "Cat_1.s2.t_3"} {
		if err := ValidUCTable(ok); err != nil {
			t.Errorf("ValidUCTable(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"", "orders", "sales.orders", "a.b.c.d", "a..c", "a.b.1c", "a.b.c-d", "a.b. c"} {
		if err := ValidUCTable(bad); err == nil {
			t.Errorf("ValidUCTable(%q) = nil, want error", bad)
		}
	}
}

func TestUnityCatalogSpecValidation(t *testing.T) {
	t.Parallel()
	st := New(t.TempDir())
	if err := st.Add(Spec{Name: "orders", Kind: KindUnityCatalog, Table: "main.sales.orders"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := st.Get("orders")
	if err != nil || got.Table != "main.sales.orders" {
		t.Fatalf("Get = %#v / %v", got, err)
	}
	bad := []Spec{
		{Name: "a", Kind: KindUnityCatalog},
		{Name: "b", Kind: KindUnityCatalog, Table: "sales.orders"},
		{Name: "c", Kind: KindUnityCatalog, Table: "m.s.o", Format: "parquet"},
		{Name: "d", Kind: KindUnityCatalog, Table: "m.s.o", Partitions: []string{"y"}},
		{Name: "e", Kind: KindUnityCatalog, Table: "m.s.o", StartFrom: "now"},
		{Name: "f", Kind: KindUnityCatalog, Table: "m.s.o", ManageBucketNotifications: true},
		{Name: "g", Kind: KindUnityCatalog, Table: "m.s.o", URL: "https://x"},
		{Name: "h", Kind: "http", URL: "https://x/y.csv", Format: "csv", Table: "m.s.o"},
	}
	for _, s := range bad {
		if err := st.Add(s); err == nil {
			t.Errorf("Add(%q) = nil, want error", s.Name)
		}
	}
}

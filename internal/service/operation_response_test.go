package service

import "testing"

// Delta's VACUUM prints on stdout before the runner's JSON answer; the
// answer is still read (pipeline optimize --vacuum failed on every local
// table with "no parseable output").
func TestOperationResponse(t *testing.T) {
	out := "Deleted 0 files and directories in a total of 1 directories.\n" +
		`{"status": "ok", "operation": "vacuum", "table": "main__sales.orders_raw", "retain_hours": 168}` + "\n"
	resp, ok := operationResponse([]byte(out))
	if !ok || resp["status"] != "ok" || resp["operation"] != "vacuum" {
		t.Fatalf("got %v %v", resp, ok)
	}
	if resp, ok := operationResponse([]byte(`{"status": "ok"}`)); !ok || resp["status"] != "ok" {
		t.Fatalf("plain answer: %v %v", resp, ok)
	}
	if _, ok := operationResponse([]byte("no json here\n")); ok {
		t.Fatal("text only: want not ok")
	}
}

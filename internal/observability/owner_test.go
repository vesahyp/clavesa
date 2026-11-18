package observability

import (
	"os"
	"os/exec"
	"reflect"
	"strings"
	"testing"
)

func TestDeadOwnerIDs(t *testing.T) {
	alive := func(pid int, start string) bool { return pid == 10 && start == "Wed Nov 18 07:50:12 2026" }
	got := deadOwnerIDs(map[string]string{
		"live":    "10:Wed Nov 18 07:50:12 2026",
		"reused":  "10:Tue Nov 17 01:00:00 2026", // same pid, other start
		"gone":    "11:Wed Nov 18 07:50:12 2026",
		"garbage": "not-a-label",
		"nopid":   ":Wed",
	}, alive)
	if want := []string{"gone", "reused"}; !reflect.DeepEqual(got, want) {
		t.Errorf("deadOwnerIDs = %v, want %v", got, want)
	}
}

// TestProcessAliveAs uses real processes: this one is alive with its own
// start time, not with another; a process that has exited is dead.
func TestProcessAliveAs(t *testing.T) {
	kv := ownerLabelKV()
	if kv == "" {
		t.Skip("ps cannot read this process's start time here")
	}
	pidStart := strings.TrimPrefix(kv, ownerLabel+"=")
	if got := deadOwnerIDs(map[string]string{"self": pidStart}, processAliveAs); len(got) != 0 {
		t.Errorf("this process counted as dead: %v", got)
	}
	_, start, _ := strings.Cut(pidStart, ":")
	if processAliveAs(os.Getpid(), "Thu Jan  1 00:00:00 1970") {
		t.Error("a different start time counted as the same process")
	}
	cmd := exec.Command("true")
	if err := cmd.Run(); err != nil {
		t.Skip(err)
	}
	if processAliveAs(cmd.Process.Pid, start) {
		t.Error("an exited process counted as alive")
	}
}

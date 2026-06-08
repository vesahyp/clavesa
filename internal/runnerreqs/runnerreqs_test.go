package runnerreqs

import (
	"strings"
	"testing"
)

func TestReadWhenAbsent(t *testing.T) {
	t.Parallel()
	got, err := Read(t.TempDir())
	if err != nil {
		t.Fatalf("Read absent: %v", err)
	}
	if got != "" {
		t.Errorf("Read absent = %q, want empty", got)
	}
}

func TestWriteReadRoundTrip(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	want := "pyasn>=1.6\ncrawlerdetect>=0.3\n"
	if err := Write(ws, want); err != nil {
		t.Fatalf("Write: %v", err)
	}
	got, err := Read(ws)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got != want {
		t.Errorf("round-trip = %q, want %q", got, want)
	}
}

func TestLinesIgnoresBlanksAndComments(t *testing.T) {
	t.Parallel()
	content := "# a comment\n\n  # indented comment\npyasn>=1.6\n  crawlerdetect  \n"
	got := Lines(content)
	want := []string{"pyasn>=1.6", "crawlerdetect"}
	if len(got) != len(want) {
		t.Fatalf("Lines = %#v, want %#v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Lines[%d] = %q, want %q", i, got[i], want[i])
		}
	}
}

func TestAddLineDedupesByPackageName(t *testing.T) {
	t.Parallel()
	content := "# deps\npyasn==1.5\n"
	out, added := AddLine(content, "pyasn>=1.6")
	if added {
		t.Errorf("AddLine same package = added true, want false")
	}
	if out != content {
		t.Errorf("AddLine no-op changed content: %q", out)
	}
	out2, added2 := AddLine(content, "crawlerdetect>=0.3")
	if !added2 {
		t.Fatalf("AddLine new package = added false, want true")
	}
	if !strings.Contains(out2, "crawlerdetect>=0.3") {
		t.Errorf("AddLine did not append: %q", out2)
	}
	// Comment preserved, original order preserved.
	if !strings.HasPrefix(out2, "# deps\npyasn==1.5\n") {
		t.Errorf("AddLine did not preserve existing lines/order: %q", out2)
	}
}

func TestRemoveLineByName(t *testing.T) {
	t.Parallel()
	content := "# deps\npyasn==1.5\ncrawlerdetect>=0.3\n"
	out, removed := RemoveLine(content, "pyasn>=99")
	if !removed {
		t.Fatalf("RemoveLine = removed false, want true")
	}
	if strings.Contains(out, "pyasn") {
		t.Errorf("RemoveLine left target: %q", out)
	}
	if !strings.Contains(out, "crawlerdetect>=0.3") || !strings.Contains(out, "# deps") {
		t.Errorf("RemoveLine dropped non-targets: %q", out)
	}
	_, removed2 := RemoveLine(content, "absent")
	if removed2 {
		t.Errorf("RemoveLine absent = removed true, want false")
	}
}

func TestTrailingNewlineNormalization(t *testing.T) {
	t.Parallel()
	// No trailing newline on input → AddLine output ends with exactly one.
	out, added := AddLine("pyasn==1.5", "crawlerdetect>=0.3")
	if !added {
		t.Fatal("expected add")
	}
	if !strings.HasSuffix(out, "crawlerdetect>=0.3\n") {
		t.Errorf("AddLine did not normalize trailing newline: %q", out)
	}
	if strings.HasSuffix(out, "\n\n") {
		t.Errorf("AddLine produced double trailing newline: %q", out)
	}
	// RemoveLine down to empty yields empty string (no stray newline).
	empty, removed := RemoveLine("pyasn==1.5\n", "pyasn")
	if !removed {
		t.Fatal("expected remove")
	}
	if empty != "" {
		t.Errorf("RemoveLine to empty = %q, want empty", empty)
	}
}

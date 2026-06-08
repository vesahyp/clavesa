package service

import "testing"

func TestRunnerRequirementsAddListRemove(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)

	// Empty workspace → no requirements.
	list, err := svc.ListRunnerRequirements()
	if err != nil {
		t.Fatalf("ListRunnerRequirements (empty): %v", err)
	}
	if len(list) != 0 {
		t.Errorf("empty workspace requirements = %#v, want none", list)
	}

	added, err := svc.AddRunnerRequirement("pyasn>=1.6")
	if err != nil || !added {
		t.Fatalf("AddRunnerRequirement = added %v / %v, want true", added, err)
	}
	// Same package, different pin → no-op.
	added2, err := svc.AddRunnerRequirement("pyasn==1.5")
	if err != nil {
		t.Fatalf("AddRunnerRequirement (dup): %v", err)
	}
	if added2 {
		t.Errorf("AddRunnerRequirement same package = added true, want false")
	}
	if _, err := svc.AddRunnerRequirement("crawlerdetect>=0.3"); err != nil {
		t.Fatalf("AddRunnerRequirement (second): %v", err)
	}

	list, err = svc.ListRunnerRequirements()
	if err != nil {
		t.Fatalf("ListRunnerRequirements: %v", err)
	}
	if len(list) != 2 {
		t.Fatalf("requirements = %#v, want 2 entries", list)
	}

	removed, err := svc.RemoveRunnerRequirement("pyasn")
	if err != nil || !removed {
		t.Fatalf("RemoveRunnerRequirement = removed %v / %v, want true", removed, err)
	}
	removedAgain, err := svc.RemoveRunnerRequirement("pyasn")
	if err != nil {
		t.Fatalf("RemoveRunnerRequirement (absent): %v", err)
	}
	if removedAgain {
		t.Errorf("RemoveRunnerRequirement absent = removed true, want false")
	}

	list, err = svc.ListRunnerRequirements()
	if err != nil {
		t.Fatalf("ListRunnerRequirements (after remove): %v", err)
	}
	if len(list) != 1 || list[0] != "crawlerdetect>=0.3" {
		t.Errorf("requirements after remove = %#v, want [crawlerdetect>=0.3]", list)
	}
}

func TestSetRunnerRequirementsRoundTrip(t *testing.T) {
	t.Parallel()
	ws := t.TempDir()
	svc := New(ws)
	want := "# my deps\npyasn>=1.6\n"
	if err := svc.SetRunnerRequirements(want); err != nil {
		t.Fatalf("SetRunnerRequirements: %v", err)
	}
	got, err := svc.RunnerRequirements()
	if err != nil {
		t.Fatalf("RunnerRequirements: %v", err)
	}
	if got != want {
		t.Errorf("round-trip = %q, want %q", got, want)
	}
}

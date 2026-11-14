package service

import (
	"strings"
	"testing"

	"github.com/vesahyp/clavesa/internal/runnerreqs"
)

func TestAddBundleRequirements(t *testing.T) {
	ws := t.TempDir()
	s := &Service{workspace: ws}
	if added, err := s.addBundleRequirements([]string{"pycountry==24.6.1", "tabulate==0.9.0"}); err != nil || len(added) != 2 {
		t.Fatal(added, err)
	}
	// The same pin again, written differently: nothing changes.
	if added, err := s.addBundleRequirements([]string{"PyCountry == 24.6.1"}); err != nil || len(added) != 0 {
		t.Errorf("same pin: %v", err)
	}
	content, _ := runnerreqs.Read(ws)
	if got := strings.Join(runnerreqs.Lines(content), ","); got != "pycountry==24.6.1,tabulate==0.9.0" {
		t.Errorf("requirements: %s", got)
	}
	if _, err := s.addBundleRequirements([]string{"tabulate==0.8.0"}); err == nil || !strings.Contains(err.Error(), "one runner image holds one version") {
		t.Errorf("a second pin of tabulate: %v", err)
	}
	if reqName("Py_Country.x>=1") != "py-country-x" {
		t.Errorf("reqName: %s", reqName("Py_Country.x>=1"))
	}
}

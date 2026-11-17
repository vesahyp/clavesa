package service

import (
	"reflect"
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

// TestHostedRaw: with hosted.personal_data, every pipeline whose schema is
// not gold is raw data (its bucket prefix and Glue database); without it,
// nothing is.
func TestHostedRaw(t *testing.T) {
	pipes := []PipelineInfo{
		{Name: "bronze", Dir: "bronze", Schema: "bronze"},
		{Name: "silver", Dir: "silver", Schema: "silver"},
		{Name: "gold", Dir: "gold", Schema: "gold"},
	}
	m := &workspace.Manifest{Name: "web-traffic", Catalog: "clavesa_web_traffic",
		Hosted: &workspace.Hosted{URL: "https://ui.clavesa.dev", PersonalData: true}}
	got := hostedRaw(m, pipes)
	want := workspace.HostedRaw{
		ObjectPatterns: []string{"bronze/*", "silver/*"},
		GlueDatabases:  []string{"clavesa_web_traffic__bronze", "clavesa_web_traffic__silver"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("hostedRaw = %+v, want %+v", got, want)
	}
	m.Hosted.PersonalData = false
	if got := hostedRaw(m, pipes); len(got.ObjectPatterns)+len(got.GlueDatabases) != 0 {
		t.Errorf("without personal_data: %+v, want nothing", got)
	}
}

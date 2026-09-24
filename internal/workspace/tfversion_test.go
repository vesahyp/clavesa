package workspace_test

import (
	"testing"

	"github.com/vesahyp/clavesa/internal/workspace"
)

func TestCheckTerraformVersion(t *testing.T) {
	cases := []struct {
		name    string
		output  string
		wantErr bool
	}{
		{name: "below floor", output: "Terraform v1.9.8\non darwin_arm64\n", wantErr: true},
		{name: "exactly at floor", output: "Terraform v1.10.0\non darwin_arm64\n", wantErr: false},
		{name: "current pin", output: "Terraform v1.14.5\non darwin_arm64\n", wantErr: false},
		{name: "newer minor", output: "Terraform v1.16.3\non darwin_arm64\n", wantErr: false},
		{name: "garbage", output: "not terraform output at all", wantErr: true},
		{name: "empty", output: "", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := workspace.CheckTerraformVersion(c.output)
			if c.wantErr && err == nil {
				t.Fatalf("CheckTerraformVersion(%q): got nil error, want one", c.output)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("CheckTerraformVersion(%q): %v", c.output, err)
			}
		})
	}
}

package service

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestHostedTreeExtras: sources lose URL query strings, fragments and
// user info; notebooks keep their outputs unless the workspace holds
// personal data; the runner requirements go; nothing nested or of another
// kind goes.
func TestHostedTreeExtras(t *testing.T) {
	for _, personal := range []bool{false, true} {
		root := t.TempDir()
		write := func(rel, body string) {
			p := filepath.Join(root, rel)
			if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		hosted := `"hosted":{"url":"https://ui.clavesa.dev","reader_principals":["arn:aws:iam::111122223333:role/x"]}`
		if personal {
			hosted = `"hosted":{"url":"https://ui.clavesa.dev","reader_principals":["arn:aws:iam::111122223333:role/x"],"personal_data":true}`
		}
		write("clavesa.json", `{"name":"w","cloud":"aws","version":1,`+hosted+`}`)
		write(".clavesa/sources/api.json", `{"kind":"http","url":"https://user:pw@api.example.com/feed.json?key=SECRET#frag","credentials":"api"}`)
		write(".clavesa/sources/nested/x.json", `{}`)
		write(".clavesa/sources/readme.txt", "x")
		write("notebooks/nb.ipynb", `{"cells":[{"cell_type":"code","source":["1+1"],"outputs":[{"output_type":"stream","text":["2"]}],"execution_count":3},{"cell_type":"markdown","source":["# hi"]}],"nbformat":4}`)
		write("notebooks/old/nb2.ipynb", `{}`)
		write(".clavesa/runner-requirements.txt", "pyasn>=1.6\n")

		files, err := hostedTreeExtras(root)
		if err != nil {
			t.Fatal(err)
		}
		got := map[string]string{}
		for _, f := range files {
			b := f.Body
			if f.Path != "" {
				b, _ = os.ReadFile(f.Path)
			}
			got[f.Key] = string(b)
		}
		if len(got) != 3 {
			t.Errorf("personal=%v: keys %v, want sources/api.json, notebooks/nb.ipynb, runner-requirements.txt", personal, keys(got))
		}
		src := got[".clavesa/sources/api.json"]
		if strings.Contains(src, "SECRET") || strings.Contains(src, "frag") || strings.Contains(src, "pw@") || !strings.Contains(src, "https://api.example.com/feed.json") || !strings.Contains(src, `"credentials": "api"`) {
			t.Errorf("source body = %s", src)
		}
		var nb struct {
			Cells []struct {
				Outputs []any `json:"outputs"`
			} `json:"cells"`
		}
		if err := json.Unmarshal([]byte(got["notebooks/nb.ipynb"]), &nb); err != nil || len(nb.Cells) != 2 {
			t.Fatalf("personal=%v: notebook %q: %v", personal, got["notebooks/nb.ipynb"], err)
		}
		if outputs := len(nb.Cells[0].Outputs); (outputs == 0) != personal {
			t.Errorf("personal=%v: code cell has %d output(s)", personal, outputs)
		}
		if got[".clavesa/runner-requirements.txt"] != "pyasn>=1.6\n" {
			t.Errorf("requirements = %q", got[".clavesa/runner-requirements.txt"])
		}
	}
}

func keys(m map[string]string) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}

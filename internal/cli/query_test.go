package cli

import "testing"

// TestFormatQueryCell pins the text-output number formatting: SQL numbers
// arrive as float64 (JSON-decoded), and a naive fmt("%v") renders anything
// ≥ ~1e6 in scientific notation. The formatter must keep integers
// integer-shaped and floats in plain decimal, matching --json.
func TestFormatQueryCell(t *testing.T) {
	cases := []struct {
		name string
		in   interface{}
		want string
	}{
		{"nil", nil, ""},
		{"small int", float64(5), "5"},
		{"big int (no sci notation)", float64(2319046), "2319046"},
		{"bigger int", float64(2964624), "2964624"},
		{"fractional", float64(3.14), "3.14"},
		{"zero", float64(0), "0"},
		{"negative", float64(-1.75), "-1.75"},
		{"string passthrough", "credit", "credit"},
		{"bool passthrough", true, "true"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := formatQueryCell(tc.in); got != tc.want {
				t.Errorf("formatQueryCell(%v) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

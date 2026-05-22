package cli

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

// printTable writes aligned columns to w. headers and each row in rows must
// have the same length.
func printTable(w io.Writer, headers []string, rows [][]string) {
	widths := make([]int, len(headers))
	for i, h := range headers {
		widths[i] = len(h)
	}
	for _, row := range rows {
		for i, cell := range row {
			if i < len(widths) && len(cell) > widths[i] {
				widths[i] = len(cell)
			}
		}
	}

	// header
	var sb strings.Builder
	for i, h := range headers {
		if i > 0 {
			sb.WriteString("  ")
		}
		if i < len(headers)-1 {
			sb.WriteString(fmt.Sprintf("%-*s", widths[i], h))
		} else {
			sb.WriteString(h)
		}
	}
	fmt.Fprintln(w, sb.String())

	// rows
	for _, row := range rows {
		sb.Reset()
		for i, cell := range row {
			if i >= len(headers) {
				break
			}
			if i > 0 {
				sb.WriteString("  ")
			}
			if i < len(headers)-1 {
				sb.WriteString(fmt.Sprintf("%-*s", widths[i], cell))
			} else {
				sb.WriteString(cell)
			}
		}
		fmt.Fprintln(w, sb.String())
	}
}

// printJSON marshals v to w as indented JSON.
func printJSON(w io.Writer, v interface{}) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

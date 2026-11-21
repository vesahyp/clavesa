package cli

import (
	"fmt"
	"io"

	"github.com/vesahyp/clavesa/internal/service"
)

// printMergeBoundWarnings prints one "warning:" line per merge output of
// the pipeline in dir whose MERGE target scan cannot prune
// (service.MergeBoundWarnings). plan and deploy go on either way.
func printMergeBoundWarnings(w io.Writer, svc *service.Service, dir string) {
	ws, err := svc.MergeBoundWarnings(dir)
	if err != nil {
		return
	}
	for _, line := range ws {
		fmt.Fprintf(w, "warning: %s\n", line)
	}
}

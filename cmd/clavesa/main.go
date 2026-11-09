package main

import (
	// procstart first: it stamps the process start time the run-timing
	// record measures the `startup` phase from.
	_ "github.com/vesahyp/clavesa/internal/procstart"

	"github.com/vesahyp/clavesa/internal/cli"
	uistatic "github.com/vesahyp/clavesa/internal/ui"
)

func main() {
	cli.SetEmbeddedUI(uistatic.FS)
	cli.Execute()
}

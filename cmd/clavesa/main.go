package main

import (
	"github.com/vesahyp/clavesa/internal/cli"
	uistatic "github.com/vesahyp/clavesa/internal/ui"
)

func main() {
	cli.SetEmbeddedUI(uistatic.FS)
	cli.Execute()
}

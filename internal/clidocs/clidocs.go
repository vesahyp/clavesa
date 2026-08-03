// Package clidocs renders the Cobra command tree into a Diátaxis-style CLI
// reference: one markdown file per command plus an index. The output is a pure
// function of the command tree — deterministic, timestamp-free — so a test can
// regenerate it and fail on drift, keeping the committed reference from rotting
// out of sync with the CLI (see cmd/docsgen and clidocs_test.go).
package clidocs

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
)

// IndexFile is the name of the generated reference index.
const IndexFile = "README.md"

// Files renders the reference for root and returns a map of filename → content.
// Filenames are flat (no directories); the caller decides where to write them.
func Files(root *cobra.Command) map[string]string {
	out := map[string]string{}
	var walk func(c *cobra.Command)
	walk = func(c *cobra.Command) {
		out[fileName(c)] = renderCommand(c)
		for _, sub := range visibleSubcommands(c) {
			walk(sub)
		}
	}
	walk(root)
	out[IndexFile] = renderIndex(root)
	return out
}

// fileName maps a command to its reference filename, e.g.
// "clavesa workspace init" → "clavesa_workspace_init.md".
func fileName(c *cobra.Command) string {
	return strings.ReplaceAll(c.CommandPath(), " ", "_") + ".md"
}

// visibleSubcommands returns the child commands that belong in the reference,
// sorted by name, skipping hidden/deprecated commands and the generated `help`.
func visibleSubcommands(c *cobra.Command) []*cobra.Command {
	var subs []*cobra.Command
	for _, sub := range c.Commands() {
		if !sub.IsAvailableCommand() || sub.Name() == "help" {
			continue
		}
		subs = append(subs, sub)
	}
	sort.Slice(subs, func(i, j int) bool { return subs[i].Name() < subs[j].Name() })
	return subs
}

func renderCommand(c *cobra.Command) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# %s\n\n", c.CommandPath())

	if s := strings.TrimSpace(c.Short); s != "" {
		fmt.Fprintf(&b, "%s\n\n", s)
	}
	if long := strings.TrimSpace(c.Long); long != "" && long != strings.TrimSpace(c.Short) {
		fmt.Fprintf(&b, "%s\n\n", long)
	}

	if c.Runnable() {
		fmt.Fprintf(&b, "## Usage\n\n```\n%s\n```\n\n", strings.TrimSpace(c.UseLine()))
	}

	if ex := strings.TrimSpace(c.Example); ex != "" {
		fmt.Fprintf(&b, "## Examples\n\n```\n%s\n```\n\n", ex)
	}

	if local := strings.TrimRight(c.NonInheritedFlags().FlagUsages(), "\n"); local != "" {
		fmt.Fprintf(&b, "## Flags\n\n```\n%s\n```\n\n", local)
	}
	if global := strings.TrimRight(c.InheritedFlags().FlagUsages(), "\n"); global != "" {
		fmt.Fprintf(&b, "## Global flags\n\n```\n%s\n```\n\n", global)
	}

	if subs := visibleSubcommands(c); len(subs) > 0 {
		b.WriteString("## Subcommands\n\n")
		for _, sub := range subs {
			fmt.Fprintf(&b, "- [%s](%s) — %s\n", sub.CommandPath(), fileName(sub), sub.Short)
		}
		b.WriteString("\n")
	}

	b.WriteString("## See also\n\n")
	if parent := c.Parent(); parent != nil {
		fmt.Fprintf(&b, "- [%s](%s) — %s\n", parent.CommandPath(), fileName(parent), parent.Short)
	}
	fmt.Fprintf(&b, "- [Command index](%s)\n", IndexFile)

	return b.String()
}

func renderIndex(root *cobra.Command) string {
	var b strings.Builder

	b.WriteString("# CLI reference\n\n")
	b.WriteString("Generated from `clavesa --help`. Do not edit by hand — run `make docs-cli` " +
		"to regenerate (a Go test fails if this directory drifts from the CLI).\n\n")
	if s := strings.TrimSpace(root.Short); s != "" {
		fmt.Fprintf(&b, "%s\n\n", s)
	}

	b.WriteString("## Commands\n\n")
	b.WriteString("| Command | Description |\n| --- | --- |\n")
	var rows func(c *cobra.Command)
	rows = func(c *cobra.Command) {
		for _, sub := range visibleSubcommands(c) {
			depth := strings.Count(sub.CommandPath(), " ") - 1
			indent := strings.Repeat("&nbsp;&nbsp;", depth)
			fmt.Fprintf(&b, "| %s[`%s`](%s) | %s |\n", indent, sub.CommandPath(), fileName(sub), sub.Short)
			rows(sub)
		}
	}
	rows(root)
	fmt.Fprintf(&b, "\nSee also [`%s`](%s) for the root command and global flags.\n",
		root.CommandPath(), fileName(root))

	return b.String()
}

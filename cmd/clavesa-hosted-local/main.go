// Command clavesa-hosted-local serves the hosted Lambda's API on this
// machine, from summary files on disk, with the SPA in front: the local
// test bench for the local-warehouse workspaces of the hosted UI
// (ADR-032 section 2). `clavesa ui --workspaces` reads workspaces live;
// it cannot show what the Lambda serves from a stored summary, which is
// what this does.
//
//	make hosted-local SUMMARIES=<dir>     # <dir>/<name>.json per workspace
//
// Each <name>.json is a published summary (with its drill-down), the
// object the Mac writes to s3://<data bucket>/summaries/<name>.json. The
// registry is every file in the directory, as kind "local". The SPA is
// served in multi-workspace read-only mode, so there is no sign-in.
// Point `make ui-phone-shots BASE_URL=http://127.0.0.1:<port>` at it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"github.com/vesahyp/clavesa/internal/hosted"
	"github.com/vesahyp/clavesa/internal/service"
	"github.com/vesahyp/clavesa/internal/ui"
	"github.com/vesahyp/clavesa/internal/workspace"
)

// dirStore is a hosted.SummaryStore over <dir>/<name>.json.
type dirStore struct{ dir string }

func (d dirStore) Get(_ context.Context, name string) ([]byte, error) {
	b, err := os.ReadFile(filepath.Join(d.dir, name+".json"))
	if os.IsNotExist(err) {
		return nil, nil
	}
	return b, err
}

func (d dirStore) Put(context.Context, string, []byte) error {
	return fmt.Errorf("read-only store")
}

func main() {
	addr := flag.String("addr", "127.0.0.1:8090", "listen address")
	dir := flag.String("summaries", "", "directory of <name>.json summaries (required)")
	flag.Parse()
	if *dir == "" {
		log.Fatal("clavesa-hosted-local: -summaries <dir> is required")
	}
	files, err := filepath.Glob(filepath.Join(*dir, "*.json"))
	if err != nil {
		log.Fatal(err)
	}
	var entries []hosted.Entry
	for _, f := range files {
		entries = append(entries, hosted.Entry{Name: strings.TrimSuffix(filepath.Base(f), ".json"), Kind: hosted.KindLocal})
	}
	raw, _ := json.Marshal(entries)
	reg, err := hosted.ParseRegistry(raw)
	if err != nil {
		log.Fatal(err)
	}
	cfg, _ := awsconfig.LoadDefaultConfig(context.Background())
	work, err := os.MkdirTemp("", "clavesa-hosted-local-")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(work)
	store := dirStore{dir: *dir}
	opener := hosted.NewWorkspaces(cfg, work)
	opener.Store = store
	api := hosted.NewServer(reg, service.HostedWellKnown{}, store, opener)

	static, err := fs.Sub(ui.FS, "dist")
	if err != nil {
		log.Fatal(err)
	}
	spa := ui.SPAHandler(static, func(r *http.Request) ui.RuntimeConfig {
		zone, _ := workspace.TimeZone("")
		c := ui.RuntimeConfig{Mode: "multi", APIBase: "/api", ReadOnly: true, TimeZone: zone}
		if rest, ok := strings.CutPrefix(r.URL.Path, "/w/"); ok {
			if name, _, _ := strings.Cut(rest, "/"); name != "" {
				c.Basename, c.APIBase, c.Workspace = "/w/"+name, "/w/"+name+"/api", name
			}
		}
		return c
	})
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.HasPrefix(p, "/api/") || p == service.HostedWellKnownPath || (strings.HasPrefix(p, "/w/") && strings.Contains(p, "/api")) {
			api.ServeHTTP(w, r)
			return
		}
		spa.ServeHTTP(w, r)
	})
	log.Printf("clavesa-hosted-local: %d workspace(s) from %s on http://%s", len(reg), *dir, *addr)
	log.Fatal(http.ListenAndServe(*addr, h))
}

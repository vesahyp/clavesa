package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/vesahyp/clavesa/internal/databricks"
	"github.com/vesahyp/clavesa/internal/hclparser"
	"github.com/vesahyp/clavesa/internal/identutil"
	"github.com/vesahyp/clavesa/internal/sources"
)

// DatabricksAccessFile is the clavesa-owned tfvars file holding the
// runner role's grants on the team's storage (ADR-027 provisional
// decision 3). Written by every pipeline deploy; never edit it by hand.
const DatabricksAccessFile = "databricks_access.auto.tfvars"

// DatabricksAccess is what the runner role needs in the team's storage.
type DatabricksAccess struct {
	Write   []string // s3:// folders of tables the pipeline writes
	Read    []string // s3:// folders of external tables it reads
	KMSKeys []string // key ARNs of those buckets (SSE-KMS)
	Notes   []string // what was looked up, for the deploy log
}

// tableGetter is the part of the Databricks client the lookup needs.
type tableGetter interface {
	GetTable(ctx context.Context, fullName string) (databricks.Table, error)
}

// newDatabricksTableGetter builds the lookup client; tests replace it.
var newDatabricksTableGetter = func(host, clientID, secret string) tableGetter {
	return databricks.NewClient(host, clientID, secret)
}

// bucketKMSKey returns the bucket's default SSE-KMS key ARN, "" for
// SSE-S3 or none. A variable so tests can replace it.
var bucketKMSKey = func(ctx context.Context, bucket string) (string, error) {
	cfg, err := awsconfig.LoadDefaultConfig(ctx)
	if err != nil {
		return "", err
	}
	out, err := s3.NewFromConfig(cfg).GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{Bucket: &bucket})
	if err != nil {
		if strings.Contains(err.Error(), "ServerSideEncryptionConfigurationNotFound") {
			return "", nil
		}
		return "", err
	}
	if out.ServerSideEncryptionConfiguration == nil {
		return "", nil
	}
	for _, r := range out.ServerSideEncryptionConfiguration.Rules {
		if r.ApplyServerSideEncryptionByDefault != nil && r.ApplyServerSideEncryptionByDefault.KMSMasterKeyID != nil {
			return *r.ApplyServerSideEncryptionByDefault.KMSMasterKeyID, nil
		}
	}
	return "", nil
}

// ucFullName mirrors the runner's unity_catalog.full_name: a full name as
// given, `schema.table` in the connection's catalog, `table` in the
// pipeline's schema, or the clavesa table name there.
func ucFullName(catalog, schema, ucTable, defaultTable string) string {
	parts := []string{}
	for _, p := range strings.Split(ucTable, ".") {
		if p != "" {
			parts = append(parts, p)
		}
	}
	switch len(parts) {
	case 3:
		return strings.Join(parts, ".")
	case 2:
		return catalog + "." + parts[0] + "." + parts[1]
	case 1:
		return catalog + "." + schema + "." + parts[0]
	}
	return catalog + "." + schema + "." + defaultTable
}

func s3Folder(loc string) string {
	loc = strings.TrimSuffix(strings.TrimSpace(loc), "/")
	if strings.HasPrefix(loc, "s3a://") {
		loc = "s3://" + strings.TrimPrefix(loc, "s3a://")
	}
	return loc
}

// ResolveDatabricksAccess works out the runner role's grants on the
// team's storage from Unity Catalog: write on the folder of every output
// table that is an existing external table or has unity_catalog_location,
// read on every external table input, and the KMS keys of their buckets.
// Managed tables need no grant (outputs are refused, inputs are read with
// vended credentials). The workspace bucket is already granted.
func (s *Service) ResolveDatabricksAccess(ctx context.Context, dir string) (DatabricksAccess, error) {
	var acc DatabricksAccess
	m, err := s.loadManifest()
	if err != nil || m.Databricks == nil {
		return acc, err
	}
	abs := s.resolveDir(dir)
	g, err := hclparser.Parse(abs)
	if err != nil {
		return acc, err
	}
	cred, err := s.databricksCredential(m.Databricks.Credential)
	if err != nil {
		return acc, err
	}
	secret, err := s.resolveSecret(ctx, cred.Secret)
	if err != nil {
		return acc, fmt.Errorf("credential %q: %w", cred.Name, err)
	}
	api := newDatabricksTableGetter(m.Databricks.Host, cred.ClientID, secret)
	pipelineName := filepath.Base(abs)
	schema := strings.ReplaceAll(identutil.Sanitize(resolvePipelineSchema(abs, pipelineName)), "-", "_")
	wsBucket := "s3://" + m.Name + "-clavesa/"

	write, read := map[string]bool{}, map[string]bool{}
	for _, n := range g.Nodes {
		if n.Type != "transform" || !nodeEnabled(n) {
			continue
		}
		keys := outputKeyList(n)
		for _, key := range keys {
			if outputFormat(n, key) == "json" {
				continue
			}
			if loc := outputStringField(n, key, "unity_catalog_location"); loc != "" {
				write[s3Folder(loc)] = true
			}
			def := n.ID
			if len(keys) > 1 || key != "default" {
				def = n.ID + "__" + key
			}
			name := ucFullName(m.Databricks.Catalog, schema, outputStringField(n, key, "unity_catalog_table"), def)
			t, err := api.GetTable(ctx, name)
			if databricks.IsNotFound(err) {
				acc.Notes = append(acc.Notes, fmt.Sprintf("%s: not in Unity Catalog yet (the first run registers it)", name))
				continue
			}
			if err != nil {
				return acc, fmt.Errorf("look up %s: %w", name, err)
			}
			if t.TableType == "EXTERNAL" && t.StorageLocation != "" {
				write[s3Folder(t.StorageLocation)] = true
				acc.Notes = append(acc.Notes, fmt.Sprintf("%s: external at %s, runner gets write", name, s3Folder(t.StorageLocation)))
			} else {
				acc.Notes = append(acc.Notes, fmt.Sprintf("%s: %s, no grant (clavesa cannot write it; see the switch runbook)", name, t.TableType))
			}
		}
		srcInputs, _ := n.Config["source_inputs"].(map[string]interface{})
		for _, v := range srcInputs {
			name := ""
			switch x := v.(type) {
			case string:
				name = strings.TrimPrefix(x, "sources.")
			case map[string]interface{}:
				name, _ = x["spec_name"].(string)
			}
			spec, err := sources.New(s.workspace).Get(name)
			if err != nil || spec.Kind != "unity-catalog" {
				continue
			}
			t, err := api.GetTable(ctx, spec.Table)
			if err != nil {
				return acc, fmt.Errorf("look up input %s: %w", spec.Table, err)
			}
			if t.TableType == "EXTERNAL" && t.StorageLocation != "" {
				read[s3Folder(t.StorageLocation)] = true
				acc.Notes = append(acc.Notes, fmt.Sprintf("%s: external input at %s, runner gets read", spec.Table, s3Folder(t.StorageLocation)))
			} else {
				acc.Notes = append(acc.Notes, fmt.Sprintf("%s: %s input, read with vended credentials", spec.Table, t.TableType))
			}
		}
	}
	for f := range write {
		if !strings.HasPrefix(f+"/", wsBucket) {
			acc.Write = append(acc.Write, f)
		}
	}
	for f := range read {
		if !write[f] && !strings.HasPrefix(f+"/", wsBucket) {
			acc.Read = append(acc.Read, f)
		}
	}
	sort.Strings(acc.Write)
	sort.Strings(acc.Read)
	buckets := map[string]bool{}
	for _, f := range append(append([]string{}, acc.Write...), acc.Read...) {
		buckets[strings.SplitN(strings.TrimPrefix(f, "s3://"), "/", 2)[0]] = true
	}
	keys := map[string]bool{}
	for bk := range buckets {
		key, err := bucketKMSKey(ctx, bk)
		if err != nil {
			acc.Notes = append(acc.Notes, fmt.Sprintf("bucket %s: could not read its encryption (%v); if it uses SSE-KMS, the runner role also needs kms:Decrypt and kms:GenerateDataKey on its key", bk, err))
			continue
		}
		if strings.HasPrefix(key, "arn:aws:kms:") && strings.Contains(key, ":key/") {
			keys[key] = true
		} else if key != "" {
			acc.Notes = append(acc.Notes, fmt.Sprintf("bucket %s: SSE-KMS key %q is not a key ARN; grant the runner role kms:Decrypt and kms:GenerateDataKey on it by hand", bk, key))
		}
	}
	for k := range keys {
		acc.KMSKeys = append(acc.KMSKeys, k)
	}
	sort.Strings(acc.KMSKeys)
	return acc, nil
}

// SyncDatabricksAccess writes DatabricksAccessFile for a pipeline. No
// connection: the file is removed. Called by every pipeline deploy, so
// the grants follow the tables Unity Catalog knows at deploy time.
func (s *Service) SyncDatabricksAccess(ctx context.Context, dir string) (DatabricksAccess, error) {
	abs := s.resolveDir(dir)
	path := filepath.Join(abs, DatabricksAccessFile)
	m, err := s.loadManifest()
	if err != nil {
		return DatabricksAccess{}, err
	}
	if m.Databricks == nil {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return DatabricksAccess{}, err
		}
		return DatabricksAccess{}, nil
	}
	acc, err := s.ResolveDatabricksAccess(ctx, dir)
	if err != nil {
		return acc, err
	}
	body := "# clavesa-managed (ADR-027): the runner role's grants on the team's storage,\n" +
		"# looked up in Unity Catalog by `clavesa pipeline deploy`. Do not edit by hand.\n" +
		"databricks_s3_write = " + hclStringList(acc.Write) + "\n" +
		"databricks_s3_read  = " + hclStringList(acc.Read) + "\n" +
		"databricks_kms_keys = " + hclStringList(acc.KMSKeys) + "\n"
	return acc, os.WriteFile(path, []byte(body), 0o644)
}

var hclSafe = regexp.MustCompile(`^[^"\\$%]*$`)

func hclStringList(items []string) string {
	q := make([]string, 0, len(items))
	for _, s := range items {
		if !hclSafe.MatchString(s) {
			s = strings.NewReplacer(`\`, `\\`, `"`, `\"`, "${", "$${", "%{", "%%{").Replace(s)
		}
		q = append(q, `"`+s+`"`)
	}
	return "[" + strings.Join(q, ", ") + "]"
}

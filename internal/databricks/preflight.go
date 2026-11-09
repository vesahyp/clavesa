package databricks

import (
	"context"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
)

// Getter is the one call the preflight needs: an authenticated GET. The
// preflight is read-only by construction: it has no other way to reach
// Databricks.
type Getter interface {
	GetJSON(ctx context.Context, path string, out any) error
}

// PreflightTarget is the step a team wants clavesa to replace, as the
// person running the preflight knows it before anything is set up.
type PreflightTarget struct {
	Host string `json:"host"`
	// Principal is the service principal clavesa will run as (its
	// application ID). Empty: the caller is that principal.
	Principal string `json:"principal,omitempty"`
	// Table is the table the replaced Databricks step writes.
	Table string `json:"table,omitempty"`
	// Shadow is the table clavesa writes next to it in shadow mode;
	// default Table + "_clavesa".
	Shadow string `json:"shadow,omitempty"`
	// ShadowLocation is the s3:// folder for the shadow table (the team's
	// bucket). Empty: clavesa's own bucket, which then needs an external
	// location the admin creates.
	ShadowLocation string `json:"shadow_location,omitempty"`
	// Inputs are Unity Catalog tables the step reads.
	Inputs []string `json:"inputs,omitempty"`
	// Job is the Databricks job that runs the step today.
	Job int64 `json:"job,omitempty"`
	// StartJobs are the jobs clavesa starts after a successful run.
	StartJobs []int64 `json:"start_jobs,omitempty"`
	// Readers read the table from SQL warehouses.
	Readers []string `json:"readers,omitempty"`
}

// Preflight check IDs. Stable: the UI keys rows and test IDs on them.
const (
	PreCaller          = "caller"
	PreExternalAccess  = "external-access"
	PreLineage         = "create-external-metadata"
	PreTable           = "table"
	PreTableFeatures   = "table-delta-features"
	PreTableGrants     = "table-grants"
	PreShadowSchema    = "shadow-schema-grants"
	PreShadowLocation  = "shadow-location"
	PreInput           = "input"
	PreJob             = "job"
	PreStartJob        = "start-job"
	PreReaders         = "readers"
	PreAWS             = "aws"
)

// Delta table features OSS Delta 4.0 (clavesa's writer) cannot write.
// A table with one of them cannot be taken over until the feature is
// dropped; Databricks commits for catalog-managed tables are the case
// that matters (managed tables created with catalog commits on).
var unwritableFeatures = map[string]string{
	"catalogManaged":       "catalog-managed commits (Databricks owns the commit protocol)",
	"catalogOwned-preview": "catalog-owned commits (Databricks owns the commit protocol)",
	"catalogOwned":         "catalog-owned commits (Databricks owns the commit protocol)",
}

// Features OSS Delta 4.0 writes; anything else is reported for a check.
var knownFeatures = map[string]bool{
	"appendOnly": true, "invariants": true, "checkConstraints": true, "changeDataFeed": true,
	"generatedColumns": true, "columnMapping": true, "identityColumns": true, "deletionVectors": true,
	"rowTracking": true, "timestampNtz": true, "domainMetadata": true, "v2Checkpoint": true,
	"liquid": true, "clustering": true, "typeWidening": true, "typeWidening-preview": true,
	"variantType": true, "variantType-preview": true, "icebergCompatV1": true, "icebergCompatV2": true,
	"vacuumProtocolCheck": true, "inCommitTimestamp": true, "allowColumnDefaults": true,
}

type ucTable struct {
	FullName         string            `json:"full_name"`
	CatalogName      string            `json:"catalog_name"`
	SchemaName       string            `json:"schema_name"`
	TableType        string            `json:"table_type"`
	DataSourceFormat string            `json:"data_source_format"`
	StorageLocation  string            `json:"storage_location"`
	Owner            string            `json:"owner"`
	Properties       map[string]string `json:"properties"`
	Columns          []struct {
		Name           string `json:"name"`
		PartitionIndex *int   `json:"partition_index"`
	} `json:"columns"`
}

type securable struct {
	Owner string `json:"owner"`
}

// Preflight checks, read-only, everything clavesa needs to replace one
// Databricks step, and returns one row per check with the exact fix. AWS
// checks are added by the caller (PreAWS rows).
func Preflight(ctx context.Context, g Getter, t PreflightTarget) []Check {
	var out []Check
	add := func(c Check) { out = append(out, c) }

	var me struct {
		UserName    string `json:"userName"`
		DisplayName string `json:"displayName"`
		ID          string `json:"id"`
	}
	if err := g.GetJSON(ctx, "/api/2.0/preview/scim/v2/Me", &me); err != nil {
		add(Check{ID: PreCaller, Title: "Sign in to " + t.Host, Status: StatusFail, Detail: err.Error(),
			Fix: "Check the host and the login: `databricks auth login --host " + t.Host + " --profile <name>`, or the service principal's client ID and secret."})
		return out
	}
	principal := t.Principal
	if principal == "" {
		principal = me.UserName
	}
	sp := "`" + principal + "`"
	add(Check{ID: PreCaller, Title: "Signed in", Status: StatusPass,
		Detail: fmt.Sprintf("as %s; checking the rights of %s", me.UserName, principal)})

	privs := func(kind, name string) (map[string]bool, error) {
		return privilegesOf(ctx, g, kind, name, principal)
	}
	owner := func(kind, name string) string {
		var s securable
		if err := g.GetJSON(ctx, "/api/2.1/unity-catalog/"+kind+"/"+url.PathEscape(name), &s); err != nil {
			return ""
		}
		return s.Owner
	}
	// need checks privileges on one securable and returns the GRANT
	// statements for what is missing ("" when nothing). Ownership covers
	// everything but the EXTERNAL USE privileges, which owners do not get
	// implicitly (P0).
	need := func(kind, sqlKind, name string, want ...string) (string, error) {
		have, err := privs(kind, name)
		if err != nil {
			return "", err
		}
		isOwner := owner(kind+"s", name) == principal
		var missing []string
		for _, w := range want {
			external := strings.HasPrefix(w, "EXTERNAL_USE")
			if have[w] || (!external && (have["ALL_PRIVILEGES"] || isOwner)) {
				continue
			}
			missing = append(missing, strings.ReplaceAll(w, "_", " "))
		}
		if len(missing) == 0 {
			return "", nil
		}
		return fmt.Sprintf("GRANT %s ON %s %s TO %s;", strings.Join(missing, ", "), sqlKind, quoteName(name), sp), nil
	}

	// Metastore: external data access and lineage.
	var ms struct {
		MetastoreID           string `json:"metastore_id"`
		ExternalAccessEnabled *bool  `json:"external_access_enabled"`
	}
	if err := g.GetJSON(ctx, "/api/2.1/unity-catalog/metastore_summary", &ms); err != nil {
		add(Check{ID: PreExternalAccess, Title: "External data access on the metastore", Status: StatusUnknown, Detail: err.Error()})
	} else if ms.ExternalAccessEnabled != nil && *ms.ExternalAccessEnabled {
		add(Check{ID: PreExternalAccess, Title: "External data access on the metastore", Status: StatusPass})
	} else {
		add(Check{ID: PreExternalAccess, Title: "External data access on the metastore", Status: StatusFail,
			Detail: "off: an outside engine cannot register tables, push lineage or get vended credentials",
			Fix:    "A metastore admin turns it on: Catalog Explorer > Metastore (gear) > External data access > Enable."})
	}
	if ms.MetastoreID != "" {
		if have, err := privs("metastore", ms.MetastoreID); err != nil {
			add(Check{ID: PreLineage, Title: "CREATE EXTERNAL METADATA (lineage)", Status: StatusUnknown, Detail: err.Error()})
		} else if have["CREATE_EXTERNAL_METADATA"] {
			add(Check{ID: PreLineage, Title: "CREATE EXTERNAL METADATA (lineage)", Status: StatusPass})
		} else {
			add(Check{ID: PreLineage, Title: "CREATE EXTERNAL METADATA (lineage)", Status: StatusFail,
				Detail: "without it clavesa's step is missing from the lineage graph (the run still works; lineage can be turned off with --push-lineage=false)",
				Fix:    "GRANT CREATE EXTERNAL METADATA ON METASTORE TO " + sp + ";"})
		}
	}

	// The replaced table.
	var tbl ucTable
	tableErr := g.GetJSON(ctx, "/api/2.1/unity-catalog/tables/"+url.PathEscape(t.Table), &tbl)
	switch {
	case t.Table == "":
		add(Check{ID: PreTable, Title: "The replaced table", Status: StatusSkipped, Detail: "no --table given"})
	case tableErr != nil:
		add(Check{ID: PreTable, Title: "The replaced table " + t.Table, Status: StatusFail, Detail: tableErr.Error(),
			Fix: "Check the name. The caller needs USE CATALOG, USE SCHEMA and SELECT to see it."})
	default:
		var parts []string
		for _, c := range tbl.Columns {
			if c.PartitionIndex != nil {
				parts = append(parts, c.Name)
			}
		}
		detail := fmt.Sprintf("%s %s at %s, owner %s", tbl.TableType, tbl.DataSourceFormat, orNone(tbl.StorageLocation), tbl.Owner)
		if len(parts) > 0 {
			detail += ", partitioned by " + strings.Join(parts, ", ") + " (clavesa output: mode = \"replace_partitions\", partition_by = [\"" + strings.Join(parts, "\", \"") + "\"])"
		} else if clustering := tbl.Properties["clusteringColumns"]; clustering != "" {
			detail += ", liquid clustering " + clustering
		}
		switch {
		case tbl.DataSourceFormat != "DELTA":
			add(Check{ID: PreTable, Title: "The replaced table " + t.Table, Status: StatusFail, Detail: detail,
				Fix: "Clavesa writes Delta tables only."})
		case tbl.TableType == "MANAGED":
			add(Check{ID: PreTable, Title: "The replaced table " + t.Table, Status: StatusPass,
				Detail: detail + ". Managed: clavesa writes a shadow table next to it and the switch renames the two (docs/cookbook/replace-a-databricks-step.md)."})
		case tbl.TableType == "EXTERNAL":
			add(Check{ID: PreTable, Title: "The replaced table " + t.Table, Status: StatusPass,
				Detail: detail + ". External: after the switch clavesa writes it in place; its folder needs the runner role's write access (AWS rows below)."})
		default:
			add(Check{ID: PreTable, Title: "The replaced table " + t.Table, Status: StatusFail, Detail: detail,
				Fix: "Clavesa replaces steps that write a table (MANAGED or EXTERNAL), not views."})
		}
		add(featureCheck(tbl))

		// Grants on the table, its schema and catalog for the switch:
		// read it (comparison, vending), and after the switch write it.
		var fixes []string
		var unknown []string
		for _, n := range []struct{ kind, sql, name string; want []string }{
			{"catalog", "CATALOG", tbl.CatalogName, []string{"USE_CATALOG"}},
			{"schema", "SCHEMA", tbl.CatalogName + "." + tbl.SchemaName, []string{"USE_SCHEMA", "CREATE_TABLE", "EXTERNAL_USE_SCHEMA"}},
			{"table", "TABLE", t.Table, []string{"SELECT", "MODIFY"}},
		} {
			fix, err := need(n.kind, n.sql, n.name, n.want...)
			if err != nil {
				unknown = append(unknown, fmt.Sprintf("%s %s: %v", n.kind, n.name, err))
				continue
			}
			if fix != "" {
				fixes = append(fixes, fix)
			}
		}
		status, detail2 := StatusPass, "USE CATALOG, USE SCHEMA, CREATE TABLE, EXTERNAL USE SCHEMA (credential vending for the comparison), SELECT and MODIFY"
		if len(unknown) > 0 {
			status, detail2 = StatusUnknown, "could not read: "+strings.Join(unknown, "; ")+" (run the preflight as an admin or as the service principal)"
		}
		if len(fixes) > 0 {
			status = StatusFail
		}
		add(Check{ID: PreTableGrants, Title: "Rights on " + t.Table + " and its parents", Status: status, Detail: detail2, Fix: strings.Join(fixes, "\n")})
	}

	// The shadow table and where it goes.
	shadow := t.Shadow
	if shadow == "" && t.Table != "" {
		shadow = t.Table + "_clavesa"
	}
	if shadow != "" {
		var existing ucTable
		err := g.GetJSON(ctx, "/api/2.1/unity-catalog/tables/"+url.PathEscape(shadow), &existing)
		switch {
		case err == nil && existing.Properties["clavesa.pipeline"] == "" && existing.TableType == "MANAGED":
			add(Check{ID: PreShadowSchema, Title: "Shadow table " + shadow, Status: StatusFail,
				Detail: "a managed table of this name exists, so it is not clavesa's", Fix: "Pick another shadow name (--shadow)."})
		case err == nil && existing.Properties["clavesa.pipeline"] == "":
			// Unity Catalog loses the REST-set marker once Databricks has
			// written the table (a history copy does); the Delta log keeps it.
			add(Check{ID: PreShadowSchema, Title: "Shadow table " + shadow, Status: StatusUnknown,
				Detail: "an external table of this name exists at " + orNone(existing.StorageLocation) + "; fine if it is clavesa's shadow table from earlier runs (the first run refuses it otherwise), else pick another name with --shadow"})
		case err != nil && !IsNotFound(err):
			add(Check{ID: PreShadowSchema, Title: "Shadow table " + shadow, Status: StatusUnknown, Detail: err.Error()})
		default:
			add(Check{ID: PreShadowSchema, Title: "Shadow table " + shadow, Status: StatusPass, Detail: "the name is free (or already clavesa's)"})
		}
	}
	if t.ShadowLocation != "" {
		var locs struct {
			ExternalLocations []ExternalLocation `json:"external_locations"`
		}
		if err := g.GetJSON(ctx, "/api/2.1/unity-catalog/external-locations?max_results=1000", &locs); err != nil {
			add(Check{ID: PreShadowLocation, Title: "External location over " + t.ShadowLocation, Status: StatusUnknown, Detail: err.Error()})
		} else {
			// The longest location URL that is a prefix of the folder.
			best, name := "", ""
			for _, l := range locs.ExternalLocations {
				u := strings.TrimSuffix(l.URL, "/") + "/"
				if strings.HasPrefix(strings.TrimSuffix(t.ShadowLocation, "/")+"/", u) && len(u) > len(best) {
					best, name = u, l.Name
				}
			}
			if name == "" {
				add(Check{ID: PreShadowLocation, Title: "External location over " + t.ShadowLocation, Status: StatusFail,
					Detail: "no external location covers it, so Unity Catalog cannot register a table there",
					Fix: fmt.Sprintf("CREATE EXTERNAL LOCATION <name> URL '%s' WITH (STORAGE CREDENTIAL <credential>);\nGRANT CREATE EXTERNAL TABLE, EXTERNAL USE LOCATION ON EXTERNAL LOCATION <name> TO %s;",
						strings.TrimSuffix(t.ShadowLocation, "/")+"/", sp)})
			} else if fix, err := need("external_location", "EXTERNAL LOCATION", name, "CREATE_EXTERNAL_TABLE", "EXTERNAL_USE_LOCATION"); err != nil {
				add(Check{ID: PreShadowLocation, Title: "External location " + name, Status: StatusUnknown, Detail: err.Error()})
			} else if fix != "" {
				add(Check{ID: PreShadowLocation, Title: "External location " + name, Status: StatusFail, Detail: "covers " + t.ShadowLocation, Fix: fix})
			} else {
				add(Check{ID: PreShadowLocation, Title: "External location " + name, Status: StatusPass, Detail: "covers " + t.ShadowLocation})
			}
		}
	}

	// Inputs.
	for _, in := range t.Inputs {
		var it ucTable
		if err := g.GetJSON(ctx, "/api/2.1/unity-catalog/tables/"+url.PathEscape(in), &it); err != nil {
			add(Check{ID: PreInput, Title: "Input " + in, Status: StatusFail, Detail: err.Error(), Fix: "Check the name; the caller needs USE CATALOG, USE SCHEMA and SELECT to see it."})
			continue
		}
		want := []string{"SELECT"}
		schemaWant := []string{"USE_SCHEMA"}
		how := "external: read from its folder; the runner role needs read on " + orNone(it.StorageLocation)
		if it.TableType == "MANAGED" {
			schemaWant = append(schemaWant, "EXTERNAL_USE_SCHEMA")
			how = "managed: read with credentials Unity Catalog vends"
		}
		var fixes []string
		var errs []string
		for _, n := range []struct{ kind, sql, name string; want []string }{
			{"catalog", "CATALOG", it.CatalogName, []string{"USE_CATALOG"}},
			{"schema", "SCHEMA", it.CatalogName + "." + it.SchemaName, schemaWant},
			{"table", "TABLE", in, want},
		} {
			fix, err := need(n.kind, n.sql, n.name, n.want...)
			if err != nil {
				errs = append(errs, err.Error())
			} else if fix != "" {
				fixes = append(fixes, fix)
			}
		}
		st := StatusPass
		if len(errs) > 0 {
			st = StatusUnknown
			how += "; could not read: " + strings.Join(errs, "; ")
		}
		if len(fixes) > 0 {
			st = StatusFail
		}
		add(Check{ID: PreInput, Title: "Input " + in, Status: st, Detail: how, Fix: strings.Join(fixes, "\n")})
	}

	// The job that runs the step today.
	if t.Job != 0 {
		add(jobCheck(ctx, g, PreJob, t.Job, principal, "CAN_VIEW",
			"clavesa checks before every write that this job is paused (two writers corrupt the table)"))
	}
	for _, j := range t.StartJobs {
		add(jobCheck(ctx, g, PreStartJob, j, principal, "CAN_MANAGE_RUN", "clavesa starts it after each successful run"))
	}

	if len(t.Readers) > 0 && t.Table != "" && tableErr == nil {
		var fixes, unknown []string
		for _, r := range t.Readers {
			for _, n := range []struct{ kind, sql, name, want string }{
				{"catalog", "CATALOG", tbl.CatalogName, "USE_CATALOG"},
				{"schema", "SCHEMA", tbl.CatalogName + "." + tbl.SchemaName, "USE_SCHEMA"},
			} {
				have, err := privilegesOf(ctx, g, n.kind, n.name, r)
				if err != nil {
					unknown = append(unknown, r+": "+err.Error())
					continue
				}
				if !have[n.want] && !have["ALL_PRIVILEGES"] && owner(n.kind+"s", n.name) != r {
					fixes = append(fixes, fmt.Sprintf("GRANT %s ON %s %s TO `%s`;", strings.ReplaceAll(n.want, "_", " "), n.sql, quoteName(n.name), r))
				}
			}
		}
		st, detail := StatusPass, "USE CATALOG and USE SCHEMA held; clavesa grants SELECT on the shadow table it registers (connect --reader)"
		if len(unknown) > 0 {
			st, detail = StatusUnknown, "could not read: "+strings.Join(unknown, "; ")
		}
		if len(fixes) > 0 {
			st, detail = StatusFail, "without USE CATALOG and USE SCHEMA a SQL warehouse refuses every query on the table, whatever its grants"
		}
		add(Check{ID: PreReaders, Title: "Readers " + strings.Join(t.Readers, ", "), Status: st, Detail: detail, Fix: strings.Join(fixes, "\n")})
	}
	return out
}

// privilegesOf is one principal's effective privileges on one securable.
func privilegesOf(ctx context.Context, g Getter, kind, name, principal string) (map[string]bool, error) {
	var out struct {
		PrivilegeAssignments []struct {
			Privileges []struct {
				Privilege string `json:"privilege"`
			} `json:"privileges"`
		} `json:"privilege_assignments"`
	}
	path := "/api/2.1/unity-catalog/effective-permissions/" + kind + "/" + url.PathEscape(name) + "?principal=" + url.QueryEscape(principal)
	if err := g.GetJSON(ctx, path, &out); err != nil {
		return nil, err
	}
	set := map[string]bool{}
	for _, pa := range out.PrivilegeAssignments {
		for _, p := range pa.Privileges {
			set[p.Privilege] = true
		}
	}
	return set, nil
}

func featureCheck(t ucTable) Check {
	var feats, bad, unknown []string
	for k, v := range t.Properties {
		if !strings.HasPrefix(k, "delta.feature.") || (v != "supported" && v != "enabled") {
			continue
		}
		f := strings.TrimPrefix(k, "delta.feature.")
		feats = append(feats, f)
		if why, ok := unwritableFeatures[f]; ok {
			bad = append(bad, f+" ("+why+")")
		} else if !knownFeatures[f] {
			unknown = append(unknown, f)
		}
	}
	sort.Strings(feats)
	sort.Strings(bad)
	sort.Strings(unknown)
	title := "Delta features of " + t.FullName
	detail := "writer version " + orNone(t.Properties["delta.minWriterVersion"]) + ", features: " + orNone(strings.Join(feats, ", "))
	switch {
	case t.TableType == "MANAGED":
		return Check{ID: PreTableFeatures, Title: title, Status: StatusPass,
			Detail: detail + ". Managed: clavesa never writes it (the shadow table takes its name at the switch), so its features do not matter."}
	case len(bad) > 0:
		return Check{ID: PreTableFeatures, Title: title, Status: StatusFail, Detail: detail,
			Fix: "Clavesa's Delta 4.0 cannot write: " + strings.Join(bad, ", ") + ". Run in shadow mode and switch by renaming (the runbook's managed-table path)."}
	case len(unknown) > 0:
		return Check{ID: PreTableFeatures, Title: title, Status: StatusUnknown,
			Detail: detail + ". Not on clavesa's list of features Delta 4.0 writes: " + strings.Join(unknown, ", ") + ". The first shadow run against a copy proves it."}
	}
	return Check{ID: PreTableFeatures, Title: title, Status: StatusPass, Detail: detail}
}

func jobCheck(ctx context.Context, g Getter, id string, job int64, principal, level, why string) Check {
	title := "Databricks job " + strconv.FormatInt(job, 10)
	var j struct {
		Settings struct {
			Name     string         `json:"name"`
			Schedule map[string]any `json:"schedule"`
			Trigger  map[string]any `json:"trigger"`
		} `json:"settings"`
	}
	if err := g.GetJSON(ctx, "/api/2.1/jobs/get?job_id="+strconv.FormatInt(job, 10), &j); err != nil {
		return Check{ID: id, Title: title, Status: StatusFail, Detail: err.Error(),
			Fix: fmt.Sprintf("Check the job ID. A job admin gives `%s` %s on it (Jobs > the job > Permissions).", principal, strings.ReplaceAll(level, "_", " "))}
	}
	title += " (" + j.Settings.Name + ")"
	state := "no schedule or trigger"
	for _, s := range []map[string]any{j.Settings.Schedule, j.Settings.Trigger} {
		if s != nil {
			ps, _ := s["pause_status"].(string)
			if ps == "" {
				ps = "UNPAUSED"
			}
			state = strings.ToLower(ps)
		}
	}
	var acl struct {
		AccessControlList []struct {
			UserName             string `json:"user_name"`
			ServicePrincipalName string `json:"service_principal_name"`
			GroupName            string `json:"group_name"`
			AllPermissions       []struct {
				PermissionLevel string `json:"permission_level"`
			} `json:"all_permissions"`
		} `json:"access_control_list"`
	}
	levels := map[string]int{"CAN_VIEW": 1, "CAN_MANAGE_RUN": 2, "IS_OWNER": 3, "CAN_MANAGE": 3}
	best := 0
	aclErr := g.GetJSON(ctx, "/api/2.0/permissions/jobs/"+strconv.FormatInt(job, 10), &acl)
	if aclErr == nil {
		for _, a := range acl.AccessControlList {
			if a.UserName != principal && a.ServicePrincipalName != principal {
				continue
			}
			for _, p := range a.AllPermissions {
				if levels[p.PermissionLevel] > best {
					best = levels[p.PermissionLevel]
				}
			}
		}
	}
	detail := why + "; schedule " + state
	switch {
	case aclErr != nil:
		return Check{ID: id, Title: title, Status: StatusUnknown, Detail: detail + "; permissions not readable: " + aclErr.Error()}
	case best >= levels[level]:
		return Check{ID: id, Title: title, Status: StatusPass, Detail: detail}
	}
	return Check{ID: id, Title: title, Status: StatusFail, Detail: detail + " (permissions granted through a group are not counted)",
		Fix: fmt.Sprintf("A job admin gives `%s` %s: Jobs > %s > Permissions.", principal, strings.ReplaceAll(level, "_", " "), j.Settings.Name)}
}

func quoteName(name string) string {
	parts := strings.Split(name, ".")
	for i, p := range parts {
		parts[i] = "`" + p + "`"
	}
	return strings.Join(parts, ".")
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

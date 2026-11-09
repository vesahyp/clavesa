package databricks

import (
	"context"
	"fmt"
	"strings"
)

// API is the part of Client the checks and the service use. Tests point
// a real Client at an httptest server; the interface exists so the
// service layer can swap the constructor, not to mock single calls.
type API interface {
	Token(ctx context.Context) (string, error)
	MetastoreSummary(ctx context.Context) (MetastoreSummary, error)
	GetCatalog(ctx context.Context, name string) (Catalog, error)
	GetExternalLocation(ctx context.Context, name string) (ExternalLocation, error)
	EffectivePrivileges(ctx context.Context, securableType, fullName, principal string) (map[string]bool, error)
}

// Check status values.
const (
	StatusPass    = "pass"
	StatusFail    = "fail"
	StatusUnknown = "unknown" // the answer could not be read; not a blocker
	StatusSkipped = "skipped" // not needed in this mode, or an earlier failure made it meaningless
)

// Check IDs. Stable: the UI keys its rows and test IDs on them.
const (
	CheckLogin            = "login"
	CheckExternalAccess   = "external-access"
	CheckExternalMetadata = "create-external-metadata"
	CheckCatalog          = "catalog"
	CheckCatalogGrants    = "catalog-grants"
	CheckStorage          = "storage"
)

// Check is one admin prerequisite and whether it holds. Fix is the exact
// step a Databricks admin takes when Status is fail; clavesa never takes
// it itself (ADR-027, "The connection and the admin's part").
type Check struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Status string `json:"status"`
	Detail string `json:"detail,omitempty"`
	Fix    string `json:"fix,omitempty"`
}

// Target is what the checks verify against.
type Target struct {
	Host     string
	ClientID string // service principal application ID
	Catalog  string
	// CreateCatalog is the --create-catalog opt-in (ADR-027 section 1):
	// clavesa's workspace stack creates the catalog, its storage and the
	// schemas. Without it the catalog must exist and is the team's.
	CreateCatalog bool
	// ExternalLocation names an admin-created location over the clavesa
	// bucket, which new tables need in the default mode.
	ExternalLocation string
}

// Privilege names as the Unity Catalog API spells them.
const (
	privAll                     = "ALL_PRIVILEGES"
	privUseCatalog              = "USE_CATALOG"
	privCreateCatalog           = "CREATE_CATALOG"
	privCreateExternalMetadata  = "CREATE_EXTERNAL_METADATA"
	privCreateStorageCredential = "CREATE_STORAGE_CREDENTIAL"
	privCreateExternalLocation  = "CREATE_EXTERNAL_LOCATION"
)

// RunChecks runs every prerequisite check in order and returns all of
// them. It never returns early on a failure the user can fix: the point is
// one list of every missing step, not one step per attempt.
func RunChecks(ctx context.Context, api API, t Target) []Check {
	sp := "`" + t.ClientID + "`"
	var out []Check

	if _, err := api.Token(ctx); err != nil {
		out = append(out, Check{
			ID: CheckLogin, Title: "Service principal can sign in", Status: StatusFail,
			Detail: err.Error(),
			Fix: fmt.Sprintf("Check the host (%s), the client ID (%s) and the secret. A Databricks account admin creates an OAuth secret for the service principal: account console > User management > Service principals > %s > Credentials & secrets > Generate secret, then puts it where the credential's secret reference points.",
				t.Host, t.ClientID, t.ClientID),
		})
		for _, c := range []struct{ id, title string }{
			{CheckExternalAccess, "External data access is on"},
			{CheckExternalMetadata, "CREATE EXTERNAL METADATA on the metastore"},
			{CheckCatalog, "Catalog"},
			{CheckCatalogGrants, "USE CATALOG on the catalog"},
			{CheckStorage, "Storage"},
		} {
			out = append(out, Check{ID: c.id, Title: c.title, Status: StatusSkipped, Detail: "sign-in failed"})
		}
		return out
	}
	out = append(out, Check{ID: CheckLogin, Title: "Service principal can sign in", Status: StatusPass})

	ms, msErr := api.MetastoreSummary(ctx)
	switch {
	case IsForbidden(msErr) && strings.Contains(msErr.Error(), "entitlement"):
		// Found in the test setup: a service principal added to the
		// workspace without an entitlement signs in, then gets 403 on
		// every Unity Catalog call.
		out = append(out, Check{
			ID: CheckExternalAccess, Title: "External data access is on", Status: StatusFail,
			Detail: "the service principal has no workspace entitlement, so every Unity Catalog call is refused",
			Fix:    fmt.Sprintf("A workspace admin gives %s the Workspace access entitlement: Settings > Identity and access > Service principals > %s > Configurations > Workspace access.", sp, t.ClientID),
		})
	case msErr != nil:
		out = append(out, Check{
			ID: CheckExternalAccess, Title: "External data access is on", Status: StatusFail,
			Detail: msErr.Error(),
			Fix:    "The workspace has no Unity Catalog metastore the service principal can see. A Databricks account admin assigns a metastore to the workspace.",
		})
	case ms.ExternalAccessEnabled == nil:
		out = append(out, Check{
			ID: CheckExternalAccess, Title: "External data access is on", Status: StatusUnknown,
			Detail: "the metastore summary does not say; check it in Catalog Explorer > Metastore details",
		})
	case !*ms.ExternalAccessEnabled:
		out = append(out, Check{
			ID: CheckExternalAccess, Title: "External data access is on", Status: StatusFail,
			Detail: fmt.Sprintf("metastore %s has external data access off", ms.MetastoreID),
			Fix:    fmt.Sprintf("A metastore admin turns it on: Catalog Explorer > Metastore details > External data access, or PATCH /api/2.1/unity-catalog/metastores/%s with {\"external_access_enabled\": true}.", ms.MetastoreID),
		})
	default:
		out = append(out, Check{ID: CheckExternalAccess, Title: "External data access is on", Status: StatusPass})
	}

	var metastorePrivs map[string]bool
	var metastoreErr error
	if msErr == nil {
		metastorePrivs, metastoreErr = api.EffectivePrivileges(ctx, "metastore", ms.MetastoreID, t.ClientID)
	} else {
		metastoreErr = msErr
	}
	out = append(out, privilegeCheck(CheckExternalMetadata, "CREATE EXTERNAL METADATA on the metastore (lineage)",
		metastorePrivs, metastoreErr, []string{privCreateExternalMetadata},
		"GRANT CREATE EXTERNAL METADATA ON METASTORE TO "+sp+";"))

	cat, catErr := api.GetCatalog(ctx, t.Catalog)
	ownCatalog := catErr == nil && cat.Owner == t.ClientID
	switch {
	case t.CreateCatalog && catErr == nil && !ownCatalog:
		out = append(out, Check{
			ID: CheckCatalog, Title: "Catalog " + t.Catalog + " (clavesa creates it)", Status: StatusFail,
			Detail: fmt.Sprintf("catalog %q already exists and belongs to %s, so clavesa cannot create it", t.Catalog, cat.Owner),
			Fix:    "Leave out --create-catalog to use the existing catalog, or name a catalog that does not exist yet.",
		})
	case t.CreateCatalog && ownCatalog:
		out = append(out, Check{ID: CheckCatalog, Title: "Catalog " + t.Catalog + " (clavesa creates it)", Status: StatusPass,
			Detail: "already created by this service principal"})
	case t.CreateCatalog && IsNotFound(catErr):
		c := privilegeCheck(CheckCatalog, "Catalog "+t.Catalog+" (clavesa creates it): CREATE CATALOG on the metastore",
			metastorePrivs, metastoreErr, []string{privCreateCatalog},
			"GRANT CREATE CATALOG ON METASTORE TO "+sp+";")
		out = append(out, c)
	case t.CreateCatalog:
		out = append(out, Check{ID: CheckCatalog, Title: "Catalog " + t.Catalog + " (clavesa creates it)", Status: StatusFail, Detail: catErr.Error()})
	case catErr != nil:
		c := Check{ID: CheckCatalog, Title: "Catalog " + t.Catalog + " exists", Status: StatusFail, Detail: catErr.Error()}
		if IsNotFound(catErr) {
			c.Detail = fmt.Sprintf("catalog %q does not exist, or the service principal cannot see it", t.Catalog)
		}
		c.Fix = fmt.Sprintf("Name an existing catalog with --catalog, grant the service principal access to it (GRANT USE CATALOG ON CATALOG `%s` TO %s;), or add --create-catalog to let clavesa create a new one.", t.Catalog, sp)
		out = append(out, c)
	default:
		out = append(out, Check{ID: CheckCatalog, Title: "Catalog " + t.Catalog + " exists", Status: StatusPass, Detail: "owner " + cat.Owner})
	}

	switch {
	case t.CreateCatalog:
		out = append(out, Check{ID: CheckCatalogGrants, Title: "USE CATALOG on the catalog", Status: StatusSkipped,
			Detail: "clavesa owns the catalog it creates"})
	case catErr != nil:
		out = append(out, Check{ID: CheckCatalogGrants, Title: "USE CATALOG on the catalog", Status: StatusSkipped, Detail: "no catalog"})
	case ownCatalog:
		out = append(out, Check{ID: CheckCatalogGrants, Title: "USE CATALOG on the catalog", Status: StatusPass, Detail: "owner"})
	default:
		privs, err := api.EffectivePrivileges(ctx, "catalog", t.Catalog, t.ClientID)
		out = append(out, privilegeCheck(CheckCatalogGrants, "USE CATALOG on the catalog",
			privs, err, []string{privUseCatalog},
			fmt.Sprintf("GRANT USE CATALOG ON CATALOG `%s` TO %s; (plus USE SCHEMA and MODIFY on the schemas and tables clavesa writes)", t.Catalog, sp)))
	}

	switch {
	case t.ExternalLocation != "":
		loc, err := api.GetExternalLocation(ctx, t.ExternalLocation)
		if err != nil {
			out = append(out, Check{
				ID: CheckStorage, Title: "External location " + t.ExternalLocation, Status: StatusFail,
				Detail: err.Error(),
				Fix: fmt.Sprintf("A Databricks admin creates external location `%s` over the clavesa workspace bucket and grants EXTERNAL USE LOCATION on it: GRANT EXTERNAL USE LOCATION ON EXTERNAL LOCATION `%s` TO %s;",
					t.ExternalLocation, t.ExternalLocation, sp),
			})
		} else {
			out = append(out, Check{ID: CheckStorage, Title: "External location " + t.ExternalLocation, Status: StatusPass, Detail: loc.URL})
		}
	case t.CreateCatalog:
		out = append(out, privilegeCheck(CheckStorage, "CREATE STORAGE CREDENTIAL and CREATE EXTERNAL LOCATION on the metastore (clavesa creates the catalog's storage)",
			metastorePrivs, metastoreErr, []string{privCreateStorageCredential, privCreateExternalLocation},
			"GRANT CREATE STORAGE CREDENTIAL, CREATE EXTERNAL LOCATION ON METASTORE TO "+sp+";"))
	default:
		out = append(out, Check{ID: CheckStorage, Title: "Storage for new tables", Status: StatusSkipped,
			Detail: "no --external-location: clavesa can write existing external tables in place, but cannot register new tables in its own bucket"})
	}
	return out
}

// privilegeCheck turns an effective-privileges answer into one Check.
// ALL_PRIVILEGES satisfies every requirement.
func privilegeCheck(id, title string, have map[string]bool, err error, need []string, fix string) Check {
	c := Check{ID: id, Title: title}
	if err != nil {
		if IsForbidden(err) {
			c.Status = StatusUnknown
			c.Detail = "the service principal may not read its own grants here: " + err.Error()
			return c
		}
		c.Status = StatusFail
		c.Detail = err.Error()
		c.Fix = fix
		return c
	}
	if have[privAll] {
		c.Status = StatusPass
		return c
	}
	var missing []string
	for _, p := range need {
		if !have[p] {
			missing = append(missing, strings.ReplaceAll(p, "_", " "))
		}
	}
	if len(missing) == 0 {
		c.Status = StatusPass
		return c
	}
	c.Status = StatusFail
	c.Detail = "missing " + strings.Join(missing, ", ")
	c.Fix = fix
	return c
}

// AllPass reports whether no check failed. Unknown and skipped do not
// block.
func AllPass(checks []Check) bool {
	for _, c := range checks {
		if c.Status == StatusFail {
			return false
		}
	}
	return true
}

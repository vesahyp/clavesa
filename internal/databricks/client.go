// Package databricks is clavesa's thin client for the Databricks REST
// APIs the Unity Catalog publish needs (ADR-027). It speaks plain
// net/http: the calls are few, and the Databricks Go SDK would pull a
// large dependency tree into the binary for them.
//
// Authentication is OAuth machine-to-machine: a service principal's
// client ID and secret are exchanged for a short-lived bearer token at
// `<host>/oidc/v1/token`.
package databricks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Client calls one Databricks workspace as one service principal.
type Client struct {
	host         string
	clientID     string
	clientSecret string
	http         *http.Client

	mu    sync.Mutex
	token string
	exp   time.Time
}

// NewClient returns a client for the workspace at host (an https URL with
// no path). The token is fetched lazily on the first call.
func NewClient(host, clientID, clientSecret string) *Client {
	return &Client{
		host:         strings.TrimRight(host, "/"),
		clientID:     clientID,
		clientSecret: clientSecret,
		http:         &http.Client{Timeout: 30 * time.Second},
	}
}

// NewTokenClient returns a client that sends a bearer token it was given,
// for example a person's token from `databricks auth token`. Used by the
// preflight, which a person runs before any service principal exists.
func NewTokenClient(host, token string) *Client {
	return &Client{
		host:  strings.TrimRight(host, "/"),
		http:  &http.Client{Timeout: 30 * time.Second},
		token: token,
		exp:   time.Now().Add(24 * time.Hour),
	}
}

// GetJSON sends one authenticated GET and decodes the answer into out.
func (c *Client) GetJSON(ctx context.Context, path string, out any) error {
	return c.do(ctx, http.MethodGet, path, nil, out)
}

// APIError is a non-2xx answer from Databricks. ErrorCode carries the
// Databricks `error_code` (e.g. "PERMISSION_DENIED") when the body had one.
type APIError struct {
	Status    int
	ErrorCode string
	Message   string
	Path      string
}

func (e *APIError) Error() string {
	if e.ErrorCode != "" {
		return fmt.Sprintf("%s: HTTP %d %s: %s", e.Path, e.Status, e.ErrorCode, e.Message)
	}
	return fmt.Sprintf("%s: HTTP %d: %s", e.Path, e.Status, e.Message)
}

// IsNotFound reports whether err is a Databricks 404.
func IsNotFound(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && ae.Status == http.StatusNotFound
}

// IsForbidden reports whether err is a Databricks 401 or 403.
func IsForbidden(err error) bool {
	var ae *APIError
	return errors.As(err, &ae) && (ae.Status == http.StatusForbidden || ae.Status == http.StatusUnauthorized)
}

// Token returns a valid bearer token, fetching a new one when the cached
// one is missing or about to expire.
func (c *Client) Token(ctx context.Context) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.token != "" && time.Now().Before(c.exp.Add(-time.Minute)) {
		return c.token, nil
	}
	form := url.Values{"grant_type": {"client_credentials"}, "scope": {"all-apis"}}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.host+"/oidc/v1/token", strings.NewReader(form.Encode()))
	if err != nil {
		return "", err
	}
	req.SetBasicAuth(c.clientID, c.clientSecret)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("oauth token: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode/100 != 2 {
		return "", &APIError{Status: resp.StatusCode, Message: strings.TrimSpace(string(body)), Path: "/oidc/v1/token"}
	}
	var tok struct {
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tok); err != nil {
		return "", fmt.Errorf("oauth token: decode: %w", err)
	}
	if tok.AccessToken == "" {
		return "", fmt.Errorf("oauth token: response has no access_token")
	}
	c.token = tok.AccessToken
	c.exp = time.Now().Add(time.Duration(tok.ExpiresIn) * time.Second)
	return c.token, nil
}

// do sends one authenticated JSON request and decodes the answer into out
// (which may be nil).
func (c *Client) do(ctx context.Context, method, path string, in, out any) error {
	tok, err := c.Token(ctx)
	if err != nil {
		return err
	}
	var body io.Reader
	if in != nil {
		data, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = strings.NewReader(string(data))
	}
	req, err := http.NewRequestWithContext(ctx, method, c.host+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if resp.StatusCode/100 != 2 {
		ae := &APIError{Status: resp.StatusCode, Path: path, Message: strings.TrimSpace(string(data))}
		var e struct {
			ErrorCode string `json:"error_code"`
			Message   string `json:"message"`
		}
		if json.Unmarshal(data, &e) == nil && e.Message != "" {
			ae.ErrorCode, ae.Message = e.ErrorCode, e.Message
		}
		return ae
	}
	if out == nil || len(data) == 0 {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("%s %s: decode: %w", method, path, err)
	}
	return nil
}

// MetastoreSummary is the subset of GET /api/2.1/unity-catalog/metastore_summary
// clavesa reads. ExternalAccessEnabled is a pointer: nil means the field
// was absent from the answer, so the check reports "unknown" instead of
// guessing.
type MetastoreSummary struct {
	MetastoreID           string `json:"metastore_id"`
	Name                  string `json:"name"`
	Region                string `json:"region"`
	Owner                 string `json:"owner"`
	ExternalAccessEnabled *bool  `json:"external_access_enabled,omitempty"`
}

// MetastoreSummary returns the metastore assigned to this workspace.
func (c *Client) MetastoreSummary(ctx context.Context) (MetastoreSummary, error) {
	var out MetastoreSummary
	err := c.do(ctx, http.MethodGet, "/api/2.1/unity-catalog/metastore_summary", nil, &out)
	return out, err
}

// Catalog is the subset of a Unity Catalog catalog clavesa reads.
type Catalog struct {
	Name  string `json:"name"`
	Owner string `json:"owner"`
}

// GetCatalog reads one catalog. A missing catalog is an IsNotFound error.
func (c *Client) GetCatalog(ctx context.Context, name string) (Catalog, error) {
	var out Catalog
	err := c.do(ctx, http.MethodGet, "/api/2.1/unity-catalog/catalogs/"+url.PathEscape(name), nil, &out)
	return out, err
}

// ExternalLocation is the subset of a Unity Catalog external location
// clavesa reads.
type ExternalLocation struct {
	Name           string `json:"name"`
	URL            string `json:"url"`
	CredentialName string `json:"credential_name"`
}

// GetExternalLocation reads one external location by name.
func (c *Client) GetExternalLocation(ctx context.Context, name string) (ExternalLocation, error) {
	var out ExternalLocation
	err := c.do(ctx, http.MethodGet, "/api/2.1/unity-catalog/external-locations/"+url.PathEscape(name), nil, &out)
	return out, err
}

// Table is the subset of a Unity Catalog table clavesa shows.
type Table struct {
	FullName         string            `json:"full_name"`
	Name             string            `json:"name"`
	TableType        string            `json:"table_type"`
	DataSourceFormat string            `json:"data_source_format"`
	StorageLocation  string            `json:"storage_location"`
	Owner            string            `json:"owner"`
	Properties       map[string]string `json:"properties,omitempty"`
	UpdatedAt        int64             `json:"updated_at,omitempty"`
}

// GetTable reads one table by its full name. A missing table (or one the
// caller cannot see) is an IsNotFound error.
func (c *Client) GetTable(ctx context.Context, fullName string) (Table, error) {
	var out Table
	err := c.do(ctx, http.MethodGet, "/api/2.1/unity-catalog/tables/"+url.PathEscape(fullName), nil, &out)
	return out, err
}

// DeleteTable removes one table's registration (an external table's files
// stay where they are).
func (c *Client) DeleteTable(ctx context.Context, fullName string) error {
	return c.do(ctx, http.MethodDelete, "/api/2.1/unity-catalog/tables/"+url.PathEscape(fullName), nil, nil)
}

// ListTables lists the tables of one schema. A missing schema is an
// IsNotFound error.
func (c *Client) ListTables(ctx context.Context, catalog, schema string) ([]Table, error) {
	var out struct {
		Tables []Table `json:"tables"`
	}
	path := "/api/2.1/unity-catalog/tables?catalog_name=" + url.QueryEscape(catalog) + "&schema_name=" + url.QueryEscape(schema) + "&max_results=200"
	err := c.do(ctx, http.MethodGet, path, nil, &out)
	return out.Tables, err
}

// EffectivePrivileges returns the privileges principal holds on one
// securable, direct or inherited, as a set of privilege names
// ("USE_CATALOG", "CREATE_SCHEMA", ...). securableType is the Unity
// Catalog path segment: "metastore", "catalog", "schema", ...
func (c *Client) EffectivePrivileges(ctx context.Context, securableType, fullName, principal string) (map[string]bool, error) {
	var out struct {
		PrivilegeAssignments []struct {
			Principal  string `json:"principal"`
			Privileges []struct {
				Privilege string `json:"privilege"`
			} `json:"privileges"`
		} `json:"privilege_assignments"`
	}
	path := "/api/2.1/unity-catalog/effective-permissions/" + securableType + "/" + url.PathEscape(fullName) +
		"?principal=" + url.QueryEscape(principal)
	if err := c.do(ctx, http.MethodGet, path, nil, &out); err != nil {
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

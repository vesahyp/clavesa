package cli

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/vesahyp/clavesa/internal/credentials"
	"github.com/vesahyp/clavesa/internal/service"
)

// newCredentialCmd implements ADR-017 slice 2's `clavesa credential`
// noun: workspace-level registry of named secrets that sources reference.
// Slice 2: `kind=header` only.
func newCredentialCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "credential",
		Short: "Manage workspace credentials (registry)",
		Long: `Manage the workspace-level credentials registry (ADR-017).

A credential is a named entry recording how to authenticate an outbound
request — never the secret material itself. Sources reference credentials
by name via --credentials.

Kinds: header (source fetches) and databricks-oauth (the Unity Catalog
publish, see clavesa workspace connect databricks). Three secret backends:
  arn:aws:secretsmanager:...   cloud-native, runtime fetch via Secrets Manager
  env:VAR_NAME                 local-only (rejected for cloud deploys)
  file:<workspace-rel>         local-only, gitignored under .clavesa/credentials/*.secret

Examples:
  clavesa credential register stripe \
    --header Authorization --value-prefix "Bearer " --secret env:STRIPE_KEY

  clavesa credential list
  clavesa credential show stripe
  clavesa credential delete stripe`,
		RunE: requireSubcommand(),
	}
	cmd.AddCommand(
		newCredentialRegisterCmd(),
		newCredentialListCmd(),
		newCredentialShowCmd(),
		newCredentialDeleteCmd(),
	)
	return cmd
}

func newCredentialRegisterCmd() *cobra.Command {
	var kind, header, prefix, secret, clientID string
	cmd := &cobra.Command{
		Use:   "register <name>",
		Short: "Register a new credential",
		Long: `Register a new credential.

Kinds:
  header            one HTTP header for a source fetch (--header, --value-prefix)
  databricks-oauth  a Databricks service principal for the Unity Catalog
                    publish (--client-id is its application ID; --secret
                    points at its OAuth secret)

Example:
  clavesa credential register dbx-sp --kind databricks-oauth \
    --client-id 6f1c...-... --secret env:DATABRICKS_CLIENT_SECRET`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			name := args[0]
			if kind == "" {
				kind = credentials.KindHeader
			}
			if secret == "" {
				return fmt.Errorf("--secret is required (arn:aws:secretsmanager:..., env:VAR, or file:<path>)")
			}
			switch kind {
			case credentials.KindHeader:
				if header == "" {
					return fmt.Errorf("--header <name> is required for kind=header")
				}
				if clientID != "" {
					return fmt.Errorf("--client-id is only valid with --kind %s", credentials.KindDatabricksOAuth)
				}
			case credentials.KindDatabricksOAuth:
				if clientID == "" {
					return fmt.Errorf("--client-id <application id> is required for kind=%s", credentials.KindDatabricksOAuth)
				}
				if header != "" || prefix != "" {
					return fmt.Errorf("--header and --value-prefix are only valid with --kind %s", credentials.KindHeader)
				}
			}
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			stored, err := svc.AddCredential(service.CredentialSpec{
				Name: name, Kind: kind,
				HeaderName: header, ValuePrefix: prefix,
				ClientID: clientID,
				Secret:   secret,
			})
			if err != nil {
				return err
			}
			fmt.Printf("Registered credential %s (%s, %s backend)\n", stored.Name, stored.Kind, stored.SecretBackend())
			return nil
		},
	}
	cmd.Flags().StringVar(&kind, "kind", credentials.KindHeader, "credential kind: header or databricks-oauth")
	cmd.Flags().StringVar(&clientID, "client-id", "", "Databricks service principal application ID (kind=databricks-oauth)")
	cmd.Flags().StringVar(&header, "header", "", "HTTP header name to inject (kind=header)")
	cmd.Flags().StringVar(&prefix, "value-prefix", "", "value prepended to the resolved secret (e.g. \"Bearer \")")
	cmd.Flags().StringVar(&secret, "secret", "", "secret reference: arn:aws:secretsmanager:..., env:VAR, or file:<workspace-rel>")
	return cmd
}

func newCredentialListCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List registered credentials",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			list, err := svc.ListCredentials()
			if err != nil {
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, credentialsJSONView(list))
			}
			if len(list) == 0 {
				fmt.Println("No credentials registered.")
				return nil
			}
			rows := make([][]string, len(list))
			for i, c := range list {
				detail := c.HeaderName
				if c.Kind == credentials.KindDatabricksOAuth {
					detail = c.ClientID
				}
				rows[i] = []string{c.Name, c.Kind, detail, c.SecretBackend()}
			}
			printTable(os.Stdout, []string{"NAME", "KIND", "HEADER / CLIENT ID", "BACKEND"}, rows)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

func newCredentialShowCmd() *cobra.Command {
	var jsonOut bool
	cmd := &cobra.Command{
		Use:   "show <name>",
		Short: "Show a credential's spec (never the secret material)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			spec, err := svc.GetCredential(args[0])
			if err != nil {
				if errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("credential %q not registered", args[0])
				}
				return err
			}
			if jsonOut {
				return printJSON(os.Stdout, credentialJSONOne(spec))
			}
			fmt.Printf("Name:    %s\n", spec.Name)
			fmt.Printf("Kind:    %s\n", spec.Kind)
			if spec.HeaderName != "" {
				fmt.Printf("Header:  %s\n", spec.HeaderName)
			}
			if spec.ValuePrefix != "" {
				fmt.Printf("Prefix:  %q\n", spec.ValuePrefix)
			}
			if spec.ClientID != "" {
				fmt.Printf("Client:  %s\n", spec.ClientID)
			}
			fmt.Printf("Backend: %s\n", spec.SecretBackend())
			fmt.Printf("Secret:  %s\n", spec.Secret)
			return nil
		},
	}
	cmd.Flags().BoolVar(&jsonOut, "json", false, "output as JSON")
	return cmd
}

// credentialJSON is the on-the-wire CLI shape for `credential list/show
// --json`. Storage's service.CredentialSpec drops Name (it's the
// filename); this view adds it back plus a derived `backend` so users
// piping to jq don't lose the identifier or have to re-parse the
// secret-prefix.
type credentialJSON struct {
	Name        string `json:"name"`
	Kind        string `json:"kind"`
	HeaderName  string `json:"header_name,omitempty"`
	ValuePrefix string `json:"value_prefix,omitempty"`
	ClientID    string `json:"client_id,omitempty"`
	Secret      string `json:"secret"`
	Backend     string `json:"backend,omitempty"`
}

func credentialJSONOne(c service.CredentialSpec) credentialJSON {
	return credentialJSON{
		Name: c.Name, Kind: c.Kind,
		HeaderName: c.HeaderName, ValuePrefix: c.ValuePrefix,
		ClientID: c.ClientID,
		Secret:   c.Secret, Backend: c.SecretBackend(),
	}
}

func credentialsJSONView(list []service.CredentialSpec) []credentialJSON {
	out := make([]credentialJSON, len(list))
	for i, c := range list {
		out[i] = credentialJSONOne(c)
	}
	return out
}

func newCredentialDeleteCmd() *cobra.Command {
	var force bool
	cmd := &cobra.Command{
		Use:   "delete <name>",
		Short: "Delete a registered credential",
		Long: `Delete a registered credential.

Refuses if any source in the workspace references it. Use --force to
delete anyway (intended for scripted teardown).`,
		Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			svc, _, err := newService(cmd)
			if err != nil {
				return err
			}
			if err := svc.DeleteCredential(args[0], force); err != nil {
				return err
			}
			fmt.Printf("Deleted credential: %s\n", args[0])
			return nil
		},
	}
	cmd.Flags().BoolVar(&force, "force", false, "delete even if sources reference this credential")
	return cmd
}

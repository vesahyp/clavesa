package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
)

// iamAllowedPrincipals is Lake Formation's stand-in for "IAM decides":
// a grant to it gives every IAM principal with Glue rights access, as if
// Lake Formation were off.
const iamAllowedPrincipals = "IAM_ALLOWED_PRINCIPALS"

// lfLockGlue and lfLockLF are the calls LakeFormationLock makes, so a
// test can stand in for AWS.
type lfLockGlue interface {
	GetDatabase(context.Context, *glue.GetDatabaseInput, ...func(*glue.Options)) (*glue.GetDatabaseOutput, error)
	UpdateDatabase(context.Context, *glue.UpdateDatabaseInput, ...func(*glue.Options)) (*glue.UpdateDatabaseOutput, error)
	GetTables(context.Context, *glue.GetTablesInput, ...func(*glue.Options)) (*glue.GetTablesOutput, error)
}

type lfLockLF interface {
	RevokePermissions(context.Context, *lakeformation.RevokePermissionsInput, ...func(*lakeformation.Options)) (*lakeformation.RevokePermissionsOutput, error)
}

// LakeFormationLockResult says what the lock changed, per database.
type LakeFormationLockResult struct {
	Database string
	// DefaultCleared is true when the database's default permissions for
	// new tables were not empty and are now.
	DefaultCleared bool
	// Revoked lists "database" and the tables whose IAM_ALLOWED_PRINCIPALS
	// grant was revoked.
	Revoked []string
}

// LakeFormationLock puts the workspace's Glue databases under Lake
// Formation (docs/ideas/column-tags.md, slice 1a), for a workspace with an
// "access" block; a no-op otherwise. For the system database and each
// deployed pipeline database:
//   - new tables get no IAM_ALLOWED_PRINCIPALS grant: the database's
//     CreateTableDefaultPermissions become empty;
//   - the IAM_ALLOWED_PRINCIPALS grant on the database and on each of its
//     tables is revoked.
//
// Then only the principals with a Lake Formation grant reach the tables:
// the runners, the runs writers, the deployer and the hosted reader, whose
// grants the pipeline and workspace stacks already make. It is idempotent
// and runs after every deploy, because a run can create a table and the
// account default gives a new database the grant again.
func (s *Service) LakeFormationLock(ctx context.Context) ([]LakeFormationLockResult, error) {
	m, err := s.loadManifest()
	if err != nil {
		return nil, err
	}
	if m == nil || m.Access == nil {
		return nil, nil
	}
	pipes, err := s.ListPipelines()
	if err != nil {
		return nil, fmt.Errorf("list pipelines: %w", err)
	}
	dbs, err := s.deployedPipelineDatabases(pipes)
	if err != nil {
		return nil, err
	}
	sys := strings.ReplaceAll(m.SystemCatalogIdentifier(), "-", "_") + "__pipelines"
	dbs = append([]string{sys}, dbs...)
	cfg, err := s.loadAWSConfig(ctx)
	if err != nil {
		return nil, err
	}
	return lakeFormationLock(ctx, glue.NewFromConfig(cfg), lakeformation.NewFromConfig(cfg), dbs)
}

func lakeFormationLock(ctx context.Context, gc lfLockGlue, lc lfLockLF, dbs []string) ([]LakeFormationLockResult, error) {
	var out []LakeFormationLockResult
	for _, db := range dbs {
		r := LakeFormationLockResult{Database: db}
		got, err := gc.GetDatabase(ctx, &glue.GetDatabaseInput{Name: aws.String(db)})
		if err != nil {
			var nf *gluetypes.EntityNotFoundException
			if errors.As(err, &nf) {
				continue // not deployed yet
			}
			return out, fmt.Errorf("glue get-database %s: %w", db, err)
		}
		d := got.Database
		if len(d.CreateTableDefaultPermissions) > 0 {
			// UpdateDatabase replaces the whole definition: send back
			// what is there, with no default permissions.
			if _, err := gc.UpdateDatabase(ctx, &glue.UpdateDatabaseInput{
				Name: aws.String(db),
				DatabaseInput: &gluetypes.DatabaseInput{
					Name:                          d.Name,
					Description:                   d.Description,
					LocationUri:                   d.LocationUri,
					Parameters:                    d.Parameters,
					TargetDatabase:                d.TargetDatabase,
					FederatedDatabase:             d.FederatedDatabase,
					CreateTableDefaultPermissions: []gluetypes.PrincipalPermissions{},
				},
			}); err != nil {
				return out, fmt.Errorf("glue update-database %s: %w", db, err)
			}
			r.DefaultCleared = true
		}
		revoked, err := revokeIAMAllowed(ctx, lc, &lftypes.Resource{Database: &lftypes.DatabaseResource{Name: aws.String(db)}})
		if err != nil {
			return out, fmt.Errorf("revoke %s on database %s: %w", iamAllowedPrincipals, db, err)
		}
		if revoked {
			r.Revoked = append(r.Revoked, "database")
		}
		var next *string
		for {
			tabs, err := gc.GetTables(ctx, &glue.GetTablesInput{DatabaseName: aws.String(db), NextToken: next})
			if err != nil {
				return out, fmt.Errorf("glue get-tables %s: %w", db, err)
			}
			for _, t := range tabs.TableList {
				revoked, err := revokeIAMAllowed(ctx, lc, &lftypes.Resource{Table: &lftypes.TableResource{DatabaseName: aws.String(db), Name: t.Name}})
				if err != nil {
					return out, fmt.Errorf("revoke %s on table %s.%s: %w", iamAllowedPrincipals, db, aws.ToString(t.Name), err)
				}
				if revoked {
					r.Revoked = append(r.Revoked, aws.ToString(t.Name))
				}
			}
			if tabs.NextToken == nil {
				break
			}
			next = tabs.NextToken
		}
		out = append(out, r)
	}
	return out, nil
}

// revokeIAMAllowed revokes IAM_ALLOWED_PRINCIPALS' ALL on a resource. A
// grant that is not there is not an error: false, nil.
func revokeIAMAllowed(ctx context.Context, lc lfLockLF, res *lftypes.Resource) (bool, error) {
	_, err := lc.RevokePermissions(ctx, &lakeformation.RevokePermissionsInput{
		Principal:   &lftypes.DataLakePrincipal{DataLakePrincipalIdentifier: aws.String(iamAllowedPrincipals)},
		Permissions: []lftypes.Permission{lftypes.PermissionAll},
		Resource:    res,
	})
	if err == nil {
		return true, nil
	}
	var inv *lftypes.InvalidInputException
	if errors.As(err, &inv) && strings.Contains(strings.ToLower(aws.ToString(inv.Message)), "no permissions") {
		return false, nil
	}
	return false, err
}

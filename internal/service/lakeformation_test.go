package service

import (
	"context"
	"sort"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/lakeformation"
	lftypes "github.com/aws/aws-sdk-go-v2/service/lakeformation/types"
)

type fakeLockGlue struct {
	dbs     map[string]*gluetypes.Database
	tables  map[string][]string
	updated map[string]*gluetypes.DatabaseInput
}

func (f *fakeLockGlue) GetDatabase(_ context.Context, in *glue.GetDatabaseInput, _ ...func(*glue.Options)) (*glue.GetDatabaseOutput, error) {
	d, ok := f.dbs[aws.ToString(in.Name)]
	if !ok {
		return nil, &gluetypes.EntityNotFoundException{Message: aws.String("not found")}
	}
	return &glue.GetDatabaseOutput{Database: d}, nil
}

func (f *fakeLockGlue) UpdateDatabase(_ context.Context, in *glue.UpdateDatabaseInput, _ ...func(*glue.Options)) (*glue.UpdateDatabaseOutput, error) {
	f.updated[aws.ToString(in.Name)] = in.DatabaseInput
	f.dbs[aws.ToString(in.Name)].CreateTableDefaultPermissions = in.DatabaseInput.CreateTableDefaultPermissions
	return &glue.UpdateDatabaseOutput{}, nil
}

func (f *fakeLockGlue) GetTables(_ context.Context, in *glue.GetTablesInput, _ ...func(*glue.Options)) (*glue.GetTablesOutput, error) {
	names := f.tables[aws.ToString(in.DatabaseName)]
	// Two pages, to cover NextToken.
	if in.NextToken == nil && len(names) > 1 {
		return &glue.GetTablesOutput{TableList: []gluetypes.Table{{Name: aws.String(names[0])}}, NextToken: aws.String("p2")}, nil
	}
	var out []gluetypes.Table
	start := 0
	if in.NextToken != nil {
		start = 1
	}
	for _, n := range names[start:] {
		out = append(out, gluetypes.Table{Name: aws.String(n)})
	}
	return &glue.GetTablesOutput{TableList: out}, nil
}

// fakeLockLF holds IAM_ALLOWED_PRINCIPALS grants as "db" or "db.table".
type fakeLockLF struct{ grants map[string]bool }

func (f *fakeLockLF) RevokePermissions(_ context.Context, in *lakeformation.RevokePermissionsInput, _ ...func(*lakeformation.Options)) (*lakeformation.RevokePermissionsOutput, error) {
	if aws.ToString(in.Principal.DataLakePrincipalIdentifier) != iamAllowedPrincipals || len(in.Permissions) != 1 || in.Permissions[0] != lftypes.PermissionAll {
		panic("unexpected revoke")
	}
	key := ""
	if in.Resource.Database != nil {
		key = aws.ToString(in.Resource.Database.Name)
	} else {
		key = aws.ToString(in.Resource.Table.DatabaseName) + "." + aws.ToString(in.Resource.Table.Name)
	}
	if !f.grants[key] {
		return nil, &lftypes.InvalidInputException{Message: aws.String("Grantee has no permissions and no grantable permissions on resource")}
	}
	delete(f.grants, key)
	return &lakeformation.RevokePermissionsOutput{}, nil
}

func TestLakeFormationLock(t *testing.T) {
	iam := []gluetypes.PrincipalPermissions{{
		Principal:   &gluetypes.DataLakePrincipal{DataLakePrincipalIdentifier: aws.String(iamAllowedPrincipals)},
		Permissions: []gluetypes.Permission{gluetypes.PermissionAll},
	}}
	g := &fakeLockGlue{
		dbs: map[string]*gluetypes.Database{
			"ws_system__pipelines": {Name: aws.String("ws_system__pipelines"), LocationUri: aws.String("s3://b/_system/pipelines/x.db"), CreateTableDefaultPermissions: iam},
			"ws__taxi":             {Name: aws.String("ws__taxi"), Description: aws.String("d"), LocationUri: aws.String("s3://b/taxi/_warehouse/ws__taxi.db"), Parameters: map[string]string{"k": "v"}, CreateTableDefaultPermissions: iam},
		},
		tables:  map[string][]string{"ws__taxi": {"trips", "revenue"}, "ws_system__pipelines": {"runs"}},
		updated: map[string]*gluetypes.DatabaseInput{},
	}
	lf := &fakeLockLF{grants: map[string]bool{"ws__taxi": true, "ws__taxi.trips": true, "ws__taxi.revenue": true, "ws_system__pipelines.runs": true}}

	res, err := lakeFormationLock(context.Background(), g, lf, []string{"ws_system__pipelines", "ws__taxi", "ws__not_deployed"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res) != 2 {
		t.Fatalf("results %+v, want 2 (the undeployed database is skipped)", res)
	}
	if len(lf.grants) != 0 {
		t.Errorf("grants left: %v", lf.grants)
	}
	taxi := g.updated["ws__taxi"]
	if taxi == nil || len(taxi.CreateTableDefaultPermissions) != 0 || aws.ToString(taxi.LocationUri) != "s3://b/taxi/_warehouse/ws__taxi.db" || aws.ToString(taxi.Description) != "d" || taxi.Parameters["k"] != "v" {
		t.Errorf("ws__taxi update lost its definition or kept defaults: %+v", taxi)
	}
	got := append([]string(nil), res[1].Revoked...)
	sort.Strings(got)
	if want := []string{"database", "revenue", "trips"}; len(got) != 3 || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("revoked on ws__taxi = %v, want %v", got, want)
	}

	// A second run changes nothing and is no error.
	g.updated = map[string]*gluetypes.DatabaseInput{}
	res, err = lakeFormationLock(context.Background(), g, lf, []string{"ws_system__pipelines", "ws__taxi"})
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range res {
		if r.DefaultCleared || len(r.Revoked) > 0 {
			t.Errorf("second run changed %+v", r)
		}
	}
	if len(g.updated) != 0 {
		t.Errorf("second run updated %v", g.updated)
	}
}

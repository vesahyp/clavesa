# `clavesa/hosted-reader/aws`

Read-only access to one clavesa workspace for the hosted UI (ADR-032,
section 2). clavesa writes the call to this module into the workspace's
`hosted.tf` while `clavesa.json` has a cloud `hosted` block
(`clavesa workspace hosted enable`), and rewrites it on every deploy and
upgrade. Do not call it by hand.

It creates, in the workspace's own account:

- the reader role `clavesa-ui-reader-<workspace>`, trusted only by
  `reader_principals` with `sts:ExternalId` = the workspace name. It reads
  the pipeline bucket, the workspace's Glue databases, state machines and
  log groups. Its one write is Athena results under
  `athena-results/clavesa-ui/`. Terraform state is explicitly denied.
- the Athena workgroup `clavesa-ui-<workspace>`, with a per-query scan
  limit.
- with `event_bus_arn`: the rules that forward run events to the hosted
  bus.
- for each database in `lf_databases`: Lake Formation grants for the
  reader role (below).

## Lake Formation

On an account where Lake Formation governs the Glue catalog (the default
on accounts created after August 2023), IAM alone does not let the reader
query: Athena fails with `Insufficient Lake Formation permission(s)`. The
module therefore always gives the reader role:

- IAM `lakeformation:GetDataAccess` (`Resource = "*"`: the action has no
  resource ARN);
- for each name in `lf_databases`: LF `DESCRIBE` on the database and
  `SELECT` + `DESCRIBE` on all its tables (wildcard).

clavesa fills `lf_databases` with the system database
`<system_catalog>__pipelines` and the `<catalog>__<schema>` database of
every pipeline that is deployed (its state has the Glue database). A
database must exist before it can be granted on, so a pipeline deployed
for the first time gets its grant when the workspace stack is applied
again: `clavesa deploy` does that in the same run.

Preconditions and behaviour:

- The deploying principal must be a Lake Formation data lake admin
  (`aws lakeformation get-data-lake-settings`). That is the same
  precondition as a pipeline deploy, whose `orchestration.tf` emits the
  same kind of grants for the pipeline roles.
- On an account that leaves the catalog to IAM (each database keeps
  `IAMAllowedPrincipals`), the grants pass through: they are accepted and
  change nothing.
- `lf_databases = []` (the default) emits no grants.

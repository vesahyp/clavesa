# clavesa workspace databricks preflight

Check, read-only, what a Databricks and an AWS admin must grant before clavesa can replace a step

Check every right and setting clavesa needs to replace one step of a
Databricks pipeline, and print the exact grant for each one missing. It
only reads: nothing in Databricks or AWS changes. It needs no clavesa
workspace, so it can run before anything is set up.

Sign in as a person (a Databricks CLI profile) and name the service
principal clavesa will run as with --principal, or sign in as that
service principal (--client-id, --secret env:VAR). A person who is not an
admin may not be able to read another principal's rights; those rows say
"??" and the grants are listed anyway. AWS checks use your AWS login
(the account clavesa deploys into); --skip-aws leaves them out.

  clavesa workspace databricks preflight \
    --host https://dbc-1234abcd-5678.cloud.databricks.com --profile work \
    --principal <service principal application id> \
    --table main.sales.daily_events --shadow-location s3://team-bucket/clavesa/daily_events/ \
    --job 123456789 --input main.raw.events --reader analysts

Exit status 0 when nothing is missing, 1 otherwise.

## Usage

```
clavesa workspace databricks preflight [flags]
```

## Flags

```
      --client-id string         sign in as this service principal (application ID) instead
      --compute string           where the step will run: fargate (checks its network and vCPU quota) or lambda (default "fargate")
      --fargate-cpu int          vCPUs of one Fargate task (the quota must allow it) (default 16)
      --host string              Databricks workspace URL (https://dbc-....cloud.databricks.com)
      --input strings            Unity Catalog tables the step reads; repeat or comma-separate
      --job int                  the Databricks job that runs the step today
      --json                     output as JSON
      --principal string         the service principal clavesa will run as (application ID); default: whoever signs in
      --profile string           Databricks CLI profile to sign in with (a person's login)
      --reader strings           users or groups that read the table from SQL warehouses
      --secret string            the service principal's OAuth secret reference: env:VAR, file:<path> or a Secrets Manager ARN
      --shadow string            the shadow table clavesa writes next to it (default <table>_clavesa)
      --shadow-location string   s3:// folder for the shadow table in the team's bucket
      --skip-aws                 leave the AWS checks out
      --start-job int64Slice     Databricks jobs clavesa starts after each successful run (default [])
      --subnet strings           subnets the Fargate task will run in (default: the default VPC's)
      --table string             the table the replaced Databricks step writes, <catalog>.<schema>.<table>
```

## Global flags

```
      --keep-metastore     leave the workspace's shared metastore container running after this command exits (env CLAVESA_KEEP_METASTORE=1)
      --workspace string   workspace root directory (default: current directory)
```

## See also

- [clavesa workspace databricks](clavesa_workspace_databricks.md) — Show the workspace's Databricks connection
- [Command index](README.md)

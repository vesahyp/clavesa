# Security policy

Clavesa is a self-hosted tool. There's no centrally-managed cloud
infrastructure — every install lives in the user's own AWS account
and on their own laptop. The scope below reflects that.

## Reporting a vulnerability

Please report security issues privately, **not** as a public GitHub
issue. Open a private security advisory:

  https://github.com/vesahyp/clavesa/security/advisories/new

This is a solo-maintained project; responses are best-effort with no
guaranteed turnaround.

## In scope

- The `clavesa` binary (Go backend + embedded UI).
- The Terraform modules under `modules/`.
- The PySpark runner image under `runner/`.

## Out of scope

- Vulnerabilities in third-party dependencies (Spark, Iceberg,
  hashicorp/aws, etc.) — please report those upstream.
- Misconfigured AWS deployments. Clavesa generates IAM scoped to the
  pipeline's needs; broader account-level posture is the operator's
  responsibility.
- Anything requiring physical access to the developer's machine.

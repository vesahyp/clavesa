package service

import "github.com/vesahyp/clavesa/internal/version"

// ModuleVersion is the Terraform module version tag referenced by all
// Clavesa modules. Bump `internal/version.Module` to release a new
// version; this alias keeps the historical `service.ModuleVersion`
// call sites working. See CLAUDE.md "Publishing to clavesa".
const ModuleVersion = version.Module

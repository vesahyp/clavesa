package workspace

import (
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// A process that reads workspaces as someone else (the hosted Lambda,
// ADR-032: an assumed reader role per workspace) registers that identity
// here, keyed by the workspace root. LoadAWSConfig then returns it for
// that root, so every AWS client a read path builds for the workspace
// (wsdeps' clients, the pipelinestatus handler, the service's own lazy
// clients) uses the assumed credentials, never the process's own role.
// One choke point, so a read path added later cannot quietly read with
// the wrong identity.

var awsOverrides sync.Map // cleaned absolute root → aws.Config

func overrideKey(root string) string {
	if abs, err := filepath.Abs(root); err == nil {
		return abs
	}
	return filepath.Clean(root)
}

// SetAWSConfigOverride makes LoadAWSConfig return cfg for the workspace at
// root. cfg normally carries a caching credential provider (an assumed
// role), so the credentials refresh by themselves.
func SetAWSConfigOverride(root string, cfg aws.Config) {
	awsOverrides.Store(overrideKey(root), cfg)
}

// ClearAWSConfigOverride removes the override for root.
func ClearAWSConfigOverride(root string) {
	awsOverrides.Delete(overrideKey(root))
}

// AWSConfigOverride returns the override for root, if one is set.
func AWSConfigOverride(root string) (aws.Config, bool) {
	v, ok := awsOverrides.Load(overrideKey(root))
	if !ok {
		return aws.Config{}, false
	}
	return v.(aws.Config), true
}

package workspace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"time"
	// The zone database is embedded: the hosted Lambda has no zoneinfo.
	_ "time/tzdata"
)

// TimeZoneEnv names the environment variable that sets the time zone the
// workspace summary counts days in and writes times in. The hosted UI
// sets it on its Lambda from its own config (hosted/infra var.time_zone),
// so the Lambda and the hosted SPA use one value.
const TimeZoneEnv = "CLAVESA_TIME_ZONE"

// TimeZone returns the IANA name of the zone a workspace counts days in:
// $CLAVESA_TIME_ZONE when set, then clavesa.json "time_zone", then the
// machine's local zone. A name that does not load is skipped. The result
// is "UTC" when nothing gives a usable name.
func TimeZone(root string) (string, *time.Location) {
	for _, name := range []string{os.Getenv(TimeZoneEnv), manifestTimeZone(root), localZoneName()} {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		if loc, err := time.LoadLocation(name); err == nil {
			return name, loc
		}
	}
	return "UTC", time.UTC
}

// manifestTimeZone reads only the "time_zone" key of clavesa.json. A pure
// read: no migration, nothing written back.
func manifestTimeZone(root string) string {
	if root == "" {
		return ""
	}
	data, err := os.ReadFile(filepath.Join(root, "clavesa.json"))
	if err != nil {
		return ""
	}
	var doc struct {
		TimeZone string `json:"time_zone"`
	}
	if json.Unmarshal(data, &doc) != nil {
		return ""
	}
	return doc.TimeZone
}

// localZoneName is the machine's zone as an IANA name: $TZ, else the
// target of /etc/localtime (".../zoneinfo/Europe/Helsinki").
func localZoneName() string {
	if tz := strings.TrimPrefix(os.Getenv("TZ"), ":"); tz != "" {
		return tz
	}
	target, err := os.Readlink("/etc/localtime")
	if err != nil {
		return ""
	}
	if i := strings.Index(target, "zoneinfo/"); i >= 0 {
		return target[i+len("zoneinfo/"):]
	}
	return ""
}

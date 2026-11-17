package hosted

import (
	"net/http"
	"strings"
)

// Raw row reads of a workspace whose data holds personal data (the
// registry's personal_data, web-traffic: visitor IPs and user agents in
// bronze and silver) are refused by the hosted API (2026-11-17 review,
// finding 2). Its pages still show runs, logs, the catalog and the gold
// tables. The local UI is not affected: there the data is the user's own.
//
// Refused routes, relative to /w/<name>/api:
//
//	/data/source, /preview/*, /sources/<name>/preview   always: raw files
//	/data/table, /data/tables/<db>/<table>/column-stats unless <db> is a
//	                                                    gold database
//
// Column statistics count as rows: a min or a max of an IP column is an
// IP address.
const personalDataRefusal = "this workspace's raw data holds personal data; the hosted UI shows rows of its gold tables only"

// servingDatabase reports whether a Glue database may show rows when the
// workspace holds personal data: only gold (<catalog>__gold), the default
// serving layer of the parity plan.
func servingDatabase(db string) bool {
	return strings.HasSuffix(db, "__gold")
}

// refusesPersonalData reports whether a request (path relative to the
// workspace's /api) reads raw rows.
func refusesPersonalData(rest string, r *http.Request) bool {
	switch {
	case rest == "/data/source", strings.HasPrefix(rest, "/preview/"):
		return true
	case strings.HasPrefix(rest, "/sources/") && strings.HasSuffix(rest, "/preview"):
		return true
	case rest == "/data/table":
		return !servingDatabase(r.URL.Query().Get("catalog_db"))
	case strings.HasPrefix(rest, "/data/tables/") && strings.HasSuffix(rest, "/column-stats"):
		parts := strings.Split(strings.TrimPrefix(rest, "/data/tables/"), "/")
		return len(parts) != 3 || !servingDatabase(parts[0])
	}
	return false
}

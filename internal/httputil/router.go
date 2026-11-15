package httputil

import "net/http"

// Router is the route-registration surface every handler's RegisterRoutes
// takes. *http.ServeMux satisfies it; ui.BuildWorkspaceMux passes a
// recording wrapper so the read-only test can walk the full route table
// (ADR-032: a new route cannot open a write without the test seeing it).
type Router interface {
	Handle(pattern string, handler http.Handler)
	HandleFunc(pattern string, handler func(http.ResponseWriter, *http.Request))
}

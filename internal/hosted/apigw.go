// Package hosted is the hosted read-only UI's server (ADR-032, slice 6):
// the code behind cmd/clavesa-hosted. It serves the same per-workspace
// API as `clavesa ui --workspaces` (ui.BuildWorkspaceMux, always behind
// ui.ReadOnlyGuard) for the workspaces in the registry, reading each
// cloud workspace through its reader role, and keeps one stored summary
// per workspace in the data bucket, refreshed by EventBridge events.
package hosted

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/aws/aws-lambda-go/events"
)

// ServeAPIGatewayV2 runs h for one HTTP API (payload format 2.0) request
// and returns its response. It is the whole adapter: the request becomes
// an *http.Request with the raw path, query, headers, cookies and body;
// the recorded response becomes the API Gateway response, base64 encoded
// when the body is not UTF-8 text.
func ServeAPIGatewayV2(ctx context.Context, h http.Handler, ev events.APIGatewayV2HTTPRequest) (events.APIGatewayV2HTTPResponse, error) {
	req, err := RequestFromAPIGatewayV2(ctx, ev)
	if err != nil {
		return events.APIGatewayV2HTTPResponse{StatusCode: http.StatusBadRequest, Body: `{"error":"bad request"}`,
			Headers: map[string]string{"Content-Type": "application/json"}}, nil
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return ResponseToAPIGatewayV2(rec.Result())
}

// RequestFromAPIGatewayV2 builds the *http.Request of one HTTP API event.
func RequestFromAPIGatewayV2(ctx context.Context, ev events.APIGatewayV2HTTPRequest) (*http.Request, error) {
	method := ev.RequestContext.HTTP.Method
	if method == "" {
		method = http.MethodGet
	}
	path := ev.RawPath
	if path == "" {
		path = ev.RequestContext.HTTP.Path
	}
	if path == "" {
		path = "/"
	}
	u := &url.URL{Path: path, RawQuery: ev.RawQueryString}
	// RawPath keeps percent-encoding the router may need (a run id in a
	// path segment).
	if p, err := url.PathUnescape(path); err == nil {
		u.Path = p
		if p != path {
			u.RawPath = path
		}
	}
	var body io.Reader = http.NoBody
	if ev.Body != "" {
		raw := []byte(ev.Body)
		if ev.IsBase64Encoded {
			dec, err := base64.StdEncoding.DecodeString(ev.Body)
			if err != nil {
				return nil, fmt.Errorf("decode base64 body: %w", err)
			}
			raw = dec
		}
		body = bytes.NewReader(raw)
	}
	// The viewer (ADR-035): the groups in the JWT claims the authorizer
	// verified. An event with no JWT authorizer (the well-known document,
	// EventBridge-made requests) carries none.
	if a := ev.RequestContext.Authorizer; a != nil && a.JWT != nil {
		ctx = withViewer(ctx, viewerFromClaims(a.JWT.Claims))
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), body)
	if err != nil {
		return nil, err
	}
	req.URL = u
	req.RequestURI = u.RequestURI()
	for k, v := range ev.Headers {
		// HTTP API joins repeated headers with a comma.
		req.Header.Set(k, v)
	}
	if len(ev.Cookies) > 0 {
		req.Header.Set("Cookie", strings.Join(ev.Cookies, "; "))
	}
	if host := ev.Headers["host"]; host != "" {
		req.Host = host
	} else if ev.RequestContext.DomainName != "" {
		req.Host = ev.RequestContext.DomainName
	}
	req.RemoteAddr = ev.RequestContext.HTTP.SourceIP
	return req, nil
}

// ResponseToAPIGatewayV2 converts a recorded response.
func ResponseToAPIGatewayV2(res *http.Response) (events.APIGatewayV2HTTPResponse, error) {
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return events.APIGatewayV2HTTPResponse{}, err
	}
	out := events.APIGatewayV2HTTPResponse{
		StatusCode:        res.StatusCode,
		Headers:           map[string]string{},
		MultiValueHeaders: map[string][]string{},
	}
	for k, vs := range res.Header {
		if strings.EqualFold(k, "Set-Cookie") {
			out.Cookies = append(out.Cookies, vs...)
			continue
		}
		if len(vs) == 1 {
			out.Headers[k] = vs[0]
		} else {
			out.MultiValueHeaders[k] = vs
		}
	}
	if utf8.Valid(raw) {
		out.Body = string(raw)
	} else {
		out.Body = base64.StdEncoding.EncodeToString(raw)
		out.IsBase64Encoded = true
	}
	return out, nil
}

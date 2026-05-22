package fileops

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

// encodeAttributeValue converts an AttributeValue to hclwrite tokens and sets
// or removes the named attribute on the provided body.
//
// Supported value types (matching the FILE-OPS protocol):
//
//	string          → HCL string literal
//	float64         → HCL number literal (JSON numbers unmarshal as float64)
//	bool            → HCL bool literal
//	nil             → attribute is removed from the block
//	ModuleReference → verbatim HCL expression (no quotes)
//	map[string]any  → HCL object literal
//	[]any           → HCL tuple literal
func encodeAttributeValue(body *hclwrite.Body, name string, value AttributeValue) error {
	if value == nil {
		body.RemoveAttribute(name)
		return nil
	}

	switch v := value.(type) {
	case string:
		body.SetAttributeValue(name, cty.StringVal(v))

	case float64:
		body.SetAttributeRaw(name, numTokens(v))

	case bool:
		body.SetAttributeValue(name, cty.BoolVal(v))

	case ModuleReference:
		body.SetAttributeRaw(name, referenceTokens(v.Expression))

	case map[string]interface{}:
		tokens, err := objectTokens(v)
		if err != nil {
			return err
		}
		body.SetAttributeRaw(name, tokens)

	case []interface{}:
		tokens, err := listTokens(v)
		if err != nil {
			return err
		}
		body.SetAttributeRaw(name, tokens)

	default:
		return fmt.Errorf("unsupported AttributeValue type %T for attribute %q", value, name)
	}

	return nil
}

// rawTokens builds a minimal hclwrite.Tokens slice for a verbatim expression.
// The leading space and trailing newline match what hclwrite produces for
// SetAttributeValue calls, keeping the formatted output consistent.
func rawTokens(expr string) hclwrite.Tokens {
	return hclwrite.Tokens{
		{
			Type:         hclsyntax.TokenIdent,
			Bytes:        []byte(expr),
			SpacesBefore: 1,
		},
		{
			Type:         hclsyntax.TokenNewline,
			Bytes:        []byte("\n"),
			SpacesBefore: 0,
		},
	}
}

// numTokens produces hclwrite tokens for a numeric literal.
func numTokens(n float64) hclwrite.Tokens {
	var s string
	if n == float64(int64(n)) {
		s = strconv.FormatInt(int64(n), 10)
	} else {
		s = strconv.FormatFloat(n, 'f', -1, 64)
	}
	return rawTokens(s)
}

// referenceTokens produces hclwrite tokens for a verbatim HCL expression.
func referenceTokens(expr string) hclwrite.Tokens {
	return rawTokens(expr)
}

// objectTokens encodes a Go map as an HCL object literal.
// Single-entry maps use inline format: { key = value }
// Multi-entry maps use multi-line format with proper newlines.
func objectTokens(m map[string]interface{}) (hclwrite.Tokens, error) {
	// Sort keys for deterministic output.
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	if len(keys) <= 1 {
		// Inline format for single-entry maps: { key = value }
		var sb strings.Builder
		sb.WriteString("{")
		for _, k := range keys {
			sb.WriteString(" ")
			sb.WriteString(k)
			sb.WriteString(" = ")
			val, err := scalarHCL(m[k])
			if err != nil {
				return nil, fmt.Errorf("object key %q: %w", k, err)
			}
			sb.WriteString(val)
		}
		sb.WriteString(" }")
		return rawTokens(sb.String()), nil
	}

	// Multi-line format for multiple entries:
	// {
	//   key1 = value1
	//   key2 = value2
	// }
	var sb strings.Builder
	sb.WriteString("{\n")
	for _, k := range keys {
		sb.WriteString("    ")
		sb.WriteString(k)
		sb.WriteString(" = ")
		val, err := scalarHCL(m[k])
		if err != nil {
			return nil, fmt.Errorf("object key %q: %w", k, err)
		}
		sb.WriteString(val)
		sb.WriteString("\n")
	}
	sb.WriteString("  }")

	return rawTokens(sb.String()), nil
}

// listTokens encodes a Go slice as an HCL tuple literal: ["a", "b"]
func listTokens(items []interface{}) (hclwrite.Tokens, error) {
	parts := make([]string, 0, len(items))
	for i, item := range items {
		s, err := scalarHCL(item)
		if err != nil {
			return nil, fmt.Errorf("list index %d: %w", i, err)
		}
		parts = append(parts, s)
	}
	expr := "[" + strings.Join(parts, ", ") + "]"
	return rawTokens(expr), nil
}

// scalarHCL converts a Go value to its HCL literal representation as a
// string fragment (used inside object/list constructors). Recurses into
// nested maps and lists so round-tripped node-config payloads (e.g.
// `output_definitions = { default = {} }` from the parser) re-encode
// cleanly without dropping nested empty objects on the floor.
func scalarHCL(v interface{}) (string, error) {
	switch val := v.(type) {
	case string:
		return strconv.Quote(val), nil
	case float64:
		if val == float64(int64(val)) {
			return strconv.FormatInt(int64(val), 10), nil
		}
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case bool:
		if val {
			return "true", nil
		}
		return "false", nil
	case ModuleReference:
		return val.Expression, nil
	case nil:
		return "null", nil
	case map[string]interface{}:
		return nestedObjectHCL(val)
	case []interface{}:
		return nestedListHCL(val)
	default:
		return "", fmt.Errorf("unsupported type %T in nested value", v)
	}
}

// nestedObjectHCL emits an inline `{ k1 = v1, k2 = v2 }` object literal
// — small enough to live inside another object/list without breaking
// readability. Empty maps emit `{}`. Sorted key order keeps output
// deterministic across runs.
func nestedObjectHCL(m map[string]interface{}) (string, error) {
	if len(m) == 0 {
		return "{}", nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		val, err := scalarHCL(m[k])
		if err != nil {
			return "", fmt.Errorf("object key %q: %w", k, err)
		}
		parts = append(parts, k+" = "+val)
	}
	return "{ " + strings.Join(parts, ", ") + " }", nil
}

// nestedListHCL emits an inline `[v1, v2]` list literal — same
// brevity-first shape as nestedObjectHCL.
func nestedListHCL(items []interface{}) (string, error) {
	if len(items) == 0 {
		return "[]", nil
	}
	parts := make([]string, 0, len(items))
	for i, item := range items {
		s, err := scalarHCL(item)
		if err != nil {
			return "", fmt.Errorf("list index %d: %w", i, err)
		}
		parts = append(parts, s)
	}
	return "[" + strings.Join(parts, ", ") + "]", nil
}

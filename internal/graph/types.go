package graph

// ValidationCode is a typed string constant identifying a specific validation failure.
type ValidationCode string

const (
	CodeCycleDetected         ValidationCode = "CYCLE_DETECTED"
	CodeDanglingReference     ValidationCode = "DANGLING_REFERENCE"
	CodeDisconnectedNode      ValidationCode = "DISCONNECTED_NODE"
	CodeMissingRequiredConfig ValidationCode = "MISSING_REQUIRED_CONFIG"
	CodeUnknownModuleSource   ValidationCode = "UNKNOWN_MODULE_SOURCE"
)

// PipelineGraph is the top-level object produced by HCL-PARSER and consumed by the UI.
type PipelineGraph struct {
	Pipeline   PipelineMeta `json:"pipeline"`
	Nodes      []Node       `json:"nodes"`
	Edges      []Edge       `json:"edges"`
	Validation Validation   `json:"validation"`
}

// PipelineMeta describes the Terraform directory that was parsed.
type PipelineMeta struct {
	Directory string   `json:"directory"`
	Files     []string `json:"files"`
}

// Node represents a single pipeline step (source, transform, or destination).
type Node struct {
	ID           string                 `json:"id"`
	Type         string                 `json:"type"`
	ModuleSource string                 `json:"module_source"`
	Config       map[string]interface{} `json:"config"`
	// PreviewSQL holds the SQL for preview execution. Mirrors config["sql"]
	// for transform nodes. Empty for source/destination nodes.
	PreviewSQL string `json:"preview_sql,omitempty"`
}

// Column is a single field within a result schema.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
}

// Edge represents a directed connection between two nodes.
type Edge struct {
	FromNode string `json:"from_node"`
	ToNode   string `json:"to_node"`
	ToInput  string `json:"to_input"`
}

// Validation holds the errors and warnings produced during graph validation.
type Validation struct {
	Errors   []ValidationMessage `json:"errors"`
	Warnings []ValidationMessage `json:"warnings"`
}

// ValidationMessage is a single error or warning from graph validation.
type ValidationMessage struct {
	Code    ValidationCode      `json:"code"`
	Message string              `json:"message"`
	Nodes   []string            `json:"nodes,omitempty"`
	Edges   []ValidationEdgeRef `json:"edges,omitempty"`
}

// ValidationEdgeRef identifies an edge involved in a validation message.
type ValidationEdgeRef struct {
	From string `json:"from"`
	To   string `json:"to"`
}

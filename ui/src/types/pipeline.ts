export type ValidationCode =
  | "CYCLE_DETECTED"
  | "DANGLING_REFERENCE"
  | "DISCONNECTED_NODE"
  | "SCHEMA_MISMATCH"
  | "MISSING_REQUIRED_CONFIG"
  | "UNKNOWN_MODULE_SOURCE";

export type Column = {
  name: string;
  type: string;
  nullable: boolean;
};

export type Node = {
  id: string;
  type: "source" | "transform" | "destination";
  module_source: string;
  config: Record<string, unknown>;
  preview_sql?: string;
};

export type Edge = {
  from_node: string;
  to_node: string;
  to_input: string;
};

export type ValidationMessage = {
  code: ValidationCode;
  message: string;
  nodes?: string[];
  edges?: Array<{ from: string; to: string }>;
};

export type Pipeline = {
  directory: string;
  files: string[];
};

export type PipelineGraph = {
  pipeline: Pipeline;
  nodes: Node[];
  edges: Edge[];
  validation: {
    errors: ValidationMessage[];
    warnings: ValidationMessage[];
  };
};

export interface ValidationResult {
  valid: boolean;
  errors: string[];
}

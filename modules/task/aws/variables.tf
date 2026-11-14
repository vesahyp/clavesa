variable "task_key" {
  description = "The task's task_key in the bundle job"
  type        = string
}

variable "task_type" {
  description = "The task type, for example notebook, sql, python, wheel, condition"
  type        = string
}

variable "parents" {
  description = "The task keys this task depends on"
  type        = list(string)
  default     = []
}

variable "enabled" {
  description = "false for a task with disabled: true in the bundle"
  type        = bool
  default     = true
}

variable "place" {
  description = "Where the task runs: clavesa or databricks"
  type        = string
  default     = "clavesa"
}

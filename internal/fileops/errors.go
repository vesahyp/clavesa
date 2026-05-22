package fileops

import "fmt"

// newError creates a *FileOpsError with the given code and a formatted message.
func newError(code FileOpsErrorCode, format string, args ...interface{}) *FileOpsError {
	return &FileOpsError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

package fileops

// FileOpsErrorCode is a typed string constant identifying a FILE-OPS error.
type FileOpsErrorCode string

const (
	ErrFileNotFound       FileOpsErrorCode = "FILE_NOT_FOUND"
	ErrBlockNotFound      FileOpsErrorCode = "BLOCK_NOT_FOUND"
	ErrBlockAlreadyExists FileOpsErrorCode = "BLOCK_ALREADY_EXISTS"
	ErrAmbiguousAddress   FileOpsErrorCode = "AMBIGUOUS_ADDRESS"
	ErrWriteFailed        FileOpsErrorCode = "WRITE_FAILED"
	ErrParseError         FileOpsErrorCode = "PARSE_ERROR"
	ErrDirectoryNotFound  FileOpsErrorCode = "DIRECTORY_NOT_FOUND"
)

// FileOpsError is the structured error type returned by all FILE-OPS operations.
type FileOpsError struct {
	Code    FileOpsErrorCode `json:"code"`
	Message string           `json:"message"`
}

func (e *FileOpsError) Error() string {
	return string(e.Code) + ": " + e.Message
}

// AttributeValue is the Go representation of any value that can appear in an
// HCL attribute. Concrete types: string, float64, bool, nil, ModuleReference,
// map[string]interface{}, []interface{}.
type AttributeValue = interface{}

// ModuleReference represents an HCL reference expression (written without quotes).
// The __type field discriminates it from plain string values in the JSON wire format.
type ModuleReference struct {
	Type       string `json:"__type"`     // Always "reference"
	Expression string `json:"expression"` // Raw HCL expression, written verbatim
}

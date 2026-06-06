// Package fileops provides safe read/write operations for Terraform (.tf) files.
// It modifies only the targeted HCL block and leaves all other content — comments,
// formatting, non-targeted blocks — completely intact.
//
// All write operations are atomic: changes are written to a temporary file in the
// same directory and then renamed over the target, so partial writes never land
// on disk.
package fileops

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
)

// FileOps exposes the four FILE-OPS operations defined in the contract.
type FileOps struct{}

// New returns a new FileOps instance.
func New() *FileOps { return &FileOps{} }

// ---------------------------------------------------------------------------
// Result types
// ---------------------------------------------------------------------------

// ReadFile is a single file entry returned by Read.
type ReadFile struct {
	Path    string `json:"path"`
	Content string `json:"content"`
}

// ReadResult is the result of a Read operation.
type ReadResult struct {
	Files []ReadFile `json:"files"`
}

// AddBlockResult is the result of an AddBlock operation.
type AddBlockResult struct {
	File         string `json:"file"`
	Content      string `json:"content"`
	BlockAddress string `json:"block_address"`
}

// WriteResult is the result of UpdateBlock and RemoveBlock.
type WriteResult struct {
	File    string `json:"file"`
	Content string `json:"content"`
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

// Read returns all .tf files and their contents from the given directory.
// Non-.tf files are ignored. Returns ErrDirectoryNotFound if the directory
// does not exist.
func (fo *FileOps) Read(directory string) (*ReadResult, error) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, newError(ErrDirectoryNotFound, "directory not found: %s", directory)
		}
		return nil, newError(ErrDirectoryNotFound, "cannot read directory %s: %v", directory, err)
	}

	var files []ReadFile
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".tf") {
			continue
		}
		absPath := filepath.Join(directory, entry.Name())
		data, err := os.ReadFile(absPath)
		if err != nil {
			return nil, newError(ErrParseError, "cannot read file %s: %v", absPath, err)
		}
		files = append(files, ReadFile{
			Path:    absPath,
			Content: string(data),
		})
	}

	if files == nil {
		files = []ReadFile{}
	}
	return &ReadResult{Files: files}, nil
}

// ---------------------------------------------------------------------------
// AddBlock
// ---------------------------------------------------------------------------

// AddBlock appends a new HCL block to the end of the file.
// Returns ErrFileNotFound if the file does not exist.
// Returns ErrBlockAlreadyExists if a block with the same type and name is
// already present in the file.
// Returns ErrParseError if the file contains invalid HCL.
// The operation is atomic.
func (fo *FileOps) AddBlock(file, blockType, blockName string, attributes map[string]AttributeValue) (*AddBlockResult, error) {
	src, err := readTFFile(file)
	if err != nil {
		return nil, err
	}

	hf, diags := hclwrite.ParseConfig(src, file, hcl_initialPos())
	if diags.HasErrors() {
		return nil, newError(ErrParseError, "HCL parse error in %s: %s", file, diags.Error())
	}

	// Check for duplicate block.
	for _, block := range hf.Body().Blocks() {
		if block.Type() == blockType && labelsMatch(block.Labels(), blockName) {
			return nil, newError(ErrBlockAlreadyExists,
				"block %s.%s already exists in %s", blockType, blockName, file)
		}
	}

	// Append the new block.
	body := hf.Body()

	// Ensure the file ends with a newline before we append, so there's a blank
	// line separating blocks.
	if len(src) > 0 && src[len(src)-1] != '\n' {
		body.AppendNewline()
	}

	newBlock := body.AppendNewBlock(blockType, []string{blockName})
	blockBody := newBlock.Body()

	if err := setAttributes(blockBody, attributes); err != nil {
		return nil, err
	}

	// Append trailing newline between blocks.
	body.AppendNewline()

	content := string(hf.Bytes())
	if err := atomicWrite(file, content); err != nil {
		return nil, err
	}

	return &AddBlockResult{
		File:         file,
		Content:      content,
		BlockAddress: blockType + "." + blockName,
	}, nil
}

// ---------------------------------------------------------------------------
// UpdateBlock
// ---------------------------------------------------------------------------

// UpdateBlock merges attributes into an existing block.
// Only the specified attributes are changed — unmentioned attributes are left
// untouched. Setting an attribute to nil removes it from the block.
// Returns ErrBlockNotFound if the address matches no block.
// Returns ErrAmbiguousAddress if the address matches more than one block.
// The operation is atomic.
func (fo *FileOps) UpdateBlock(file, blockAddress string, attributes map[string]AttributeValue) (*WriteResult, error) {
	src, err := readTFFile(file)
	if err != nil {
		return nil, err
	}

	hf, diags := hclwrite.ParseConfig(src, file, hcl_initialPos())
	if diags.HasErrors() {
		return nil, newError(ErrParseError, "HCL parse error in %s: %s", file, diags.Error())
	}

	blockType, blockName, parseErr := parseBlockAddress(blockAddress)
	if parseErr != nil {
		return nil, newError(ErrBlockNotFound, "invalid block address %q", blockAddress)
	}

	targets := findBlocks(hf.Body(), blockType, blockName)
	switch len(targets) {
	case 0:
		return nil, newError(ErrBlockNotFound, "block %s not found in %s", blockAddress, file)
	case 1:
		// expected — proceed
	default:
		return nil, newError(ErrAmbiguousAddress,
			"block address %s matches %d blocks in %s", blockAddress, len(targets), file)
	}

	if err := setAttributes(targets[0].Body(), attributes); err != nil {
		return nil, err
	}

	content := string(hf.Bytes())
	if err := atomicWrite(file, content); err != nil {
		return nil, err
	}

	return &WriteResult{File: file, Content: content}, nil
}

// ---------------------------------------------------------------------------
// RemoveBlock
// ---------------------------------------------------------------------------

// RemoveBlock removes an entire block from the file, including any blank lines
// immediately following it.
// Returns ErrBlockNotFound if the address matches no block.
// Returns ErrAmbiguousAddress if the address matches more than one block.
// The operation is atomic.
func (fo *FileOps) RemoveBlock(file, blockAddress string) (*WriteResult, error) {
	src, err := readTFFile(file)
	if err != nil {
		return nil, err
	}

	hf, diags := hclwrite.ParseConfig(src, file, hcl_initialPos())
	if diags.HasErrors() {
		return nil, newError(ErrParseError, "HCL parse error in %s: %s", file, diags.Error())
	}

	blockType, blockName, parseErr := parseBlockAddress(blockAddress)
	if parseErr != nil {
		return nil, newError(ErrBlockNotFound, "invalid block address %q", blockAddress)
	}

	targets := findBlocks(hf.Body(), blockType, blockName)
	switch len(targets) {
	case 0:
		return nil, newError(ErrBlockNotFound, "block %s not found in %s", blockAddress, file)
	case 1:
		// expected — proceed
	default:
		return nil, newError(ErrAmbiguousAddress,
			"block address %s matches %d blocks in %s", blockAddress, len(targets), file)
	}

	hf.Body().RemoveBlock(targets[0])

	content := string(hf.Bytes())
	if err := atomicWrite(file, content); err != nil {
		return nil, err
	}

	return &WriteResult{File: file, Content: content}, nil
}

// ---------------------------------------------------------------------------
// WriteFile
// ---------------------------------------------------------------------------

// WriteFile atomically writes content to path. It is for callers that edit
// HCL with hclwrite directly — e.g. renaming a block label, which the
// attribute-level Update/Add/Remove operations can't express — and still
// want the same atomic-write guarantee.
func (fo *FileOps) WriteFile(path, content string) error {
	return atomicWrite(path, content)
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

// readTFFile reads a .tf file and returns its bytes, or a FileOpsError if it
// cannot be read.
func readTFFile(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, newError(ErrFileNotFound, "file not found: %s", path)
		}
		return nil, newError(ErrParseError, "cannot read file %s: %v", path, err)
	}
	return data, nil
}

// hcl_initialPos returns the initial source position used by hclwrite.ParseConfig.
func hcl_initialPos() hcl.Pos {
	return hcl.Pos{Line: 1, Column: 1}
}

// parseBlockAddress splits "type.name" into its two parts.
func parseBlockAddress(address string) (blockType, blockName string, err error) {
	parts := strings.SplitN(address, ".", 2)
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return "", "", fmt.Errorf("invalid block address: %q", address)
	}
	return parts[0], parts[1], nil
}

// labelsMatch checks whether a block's labels contain exactly the given single label.
func labelsMatch(labels []string, name string) bool {
	return len(labels) == 1 && labels[0] == name
}

// findBlocks returns all blocks in body that match blockType and blockName.
func findBlocks(body *hclwrite.Body, blockType, blockName string) []*hclwrite.Block {
	var result []*hclwrite.Block
	for _, block := range body.Blocks() {
		if block.Type() == blockType && labelsMatch(block.Labels(), blockName) {
			result = append(result, block)
		}
	}
	return result
}

// attributeLeadOrder is the canonical lead order for the common module
// identity attributes. hclwrite column-aligns `=` within a block and the
// width depends on the order attributes are added, so a stable order is
// what makes re-emitting an unchanged block a no-op diff. Identity attrs
// come first in this fixed order; everything else follows alphabetically
// (see sortedAttributeNames). Grounded in service.AddNode's attr map.
var attributeLeadOrder = []string{
	"source",
	"name",
	"pipeline_name",
	"bucket",
	"catalog",
	"schema",
	"system_catalog",
}

// sortedAttributeNames returns the attribute names in a deterministic order:
// the canonical lead attributes first (attributeLeadOrder), then the rest
// alphabetically. Iterating the attributes map directly is non-deterministic
// (Go randomizes map iteration), which previously produced noisy `.tf`
// diffs and flaky exact-HCL assertions.
func sortedAttributeNames(attributes map[string]AttributeValue) []string {
	lead := make([]string, 0, len(attributeLeadOrder))
	leadSet := make(map[string]bool, len(attributeLeadOrder))
	for _, name := range attributeLeadOrder {
		if _, ok := attributes[name]; ok {
			lead = append(lead, name)
			leadSet[name] = true
		}
	}
	rest := make([]string, 0, len(attributes))
	for name := range attributes {
		if !leadSet[name] {
			rest = append(rest, name)
		}
	}
	sort.Strings(rest)
	return append(lead, rest...)
}

// setAttributes applies the attributes map to the block body.
// nil values remove the attribute; all others set/overwrite it.
// Attributes are encoded in a deterministic order (see sortedAttributeNames)
// so the emitted HCL — including hclwrite's `=` column alignment — is stable
// across runs.
func setAttributes(body *hclwrite.Body, attributes map[string]AttributeValue) error {
	for _, name := range sortedAttributeNames(attributes) {
		if err := encodeAttributeValue(body, name, attributes[name]); err != nil {
			return newError(ErrParseError, "cannot encode attribute %q: %v", name, err)
		}
	}
	return nil
}

// atomicWrite writes content to a temporary file in the same directory as path,
// then renames the temp file over path. This ensures the write is atomic from
// the OS perspective.
func atomicWrite(path, content string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".fileops-*.tf.tmp")
	if err != nil {
		return newError(ErrWriteFailed, "cannot create temp file: %v", err)
	}
	tmpName := tmp.Name()

	if _, err := tmp.WriteString(content); err != nil {
		tmp.Close()
		os.Remove(tmpName)
		return newError(ErrWriteFailed, "cannot write temp file: %v", err)
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmpName)
		return newError(ErrWriteFailed, "cannot close temp file: %v", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		os.Remove(tmpName)
		return newError(ErrWriteFailed, "cannot rename temp file: %v", err)
	}
	return nil
}

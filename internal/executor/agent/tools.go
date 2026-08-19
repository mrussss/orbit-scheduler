package agent

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/mrussss/orbit-scheduler/internal/executor/llm"
)

const (
	ToolSearchCode = "search_code"
	ToolReadFile   = "read_file"
	ToolReadDocs   = "read_docs"
)

type ToolLimits struct {
	MaxFileBytes   int64
	MaxResultBytes int
	MaxMatches     int
}

type Toolbox struct {
	roots  map[string]string
	limits ToolLimits
}

func NewToolbox(repositories map[string]string, limits ToolLimits) (*Toolbox, error) {
	if len(repositories) == 0 || limits.MaxFileBytes <= 0 || limits.MaxResultBytes <= 0 || limits.MaxMatches <= 0 {
		return nil, errors.New("invalid agent toolbox configuration")
	}
	roots := make(map[string]string, len(repositories))
	for alias, root := range repositories {
		if alias == "" || strings.ContainsAny(alias, `/\\`) || !filepath.IsAbs(root) {
			return nil, fmt.Errorf("invalid repository allowlist entry %q", alias)
		}
		canonical, err := filepath.EvalSymlinks(root)
		if err != nil {
			return nil, fmt.Errorf("resolve repository %q: %w", alias, err)
		}
		info, err := os.Stat(canonical)
		if err != nil || !info.IsDir() {
			return nil, fmt.Errorf("repository %q is not a directory", alias)
		}
		roots[alias] = filepath.Clean(canonical)
	}
	return &Toolbox{roots: roots, limits: limits}, nil
}

func ToolDefinitions() []llm.ToolDefinition {
	return []llm.ToolDefinition{
		{Name: ToolSearchCode, Description: "Search for a literal text fragment in allowlisted source files.", Parameters: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"query":{"type":"string"},"path":{"type":"string"}},"required":["query"]}`)},
		{Name: ToolReadFile, Description: "Read a bounded line range from one allowlisted text source file.", Parameters: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"path":{"type":"string"},"start_line":{"type":"integer","minimum":1},"end_line":{"type":"integer","minimum":1}},"required":["path"]}`)},
		{Name: ToolReadDocs, Description: "Search allowlisted repository documentation for a literal text fragment.", Parameters: json.RawMessage(`{"type":"object","additionalProperties":false,"properties":{"query":{"type":"string"},"path":{"type":"string"}},"required":["query"]}`)},
	}
}

type searchArgs struct {
	Query string `json:"query"`
	Path  string `json:"path,omitempty"`
}

type readArgs struct {
	Path      string `json:"path"`
	StartLine int    `json:"start_line,omitempty"`
	EndLine   int    `json:"end_line,omitempty"`
}

type match struct {
	Path string `json:"path"`
	Line int    `json:"line"`
	Text string `json:"text"`
}

func (t *Toolbox) Execute(ctx context.Context, repository, name string, raw json.RawMessage) (json.RawMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root, ok := t.roots[repository]
	if !ok {
		return nil, errors.New("repository_root is not allowlisted")
	}
	var result any
	switch name {
	case ToolSearchCode, ToolReadDocs:
		var args searchArgs
		if err := decodeStrict(raw, &args); err != nil {
			return nil, fmt.Errorf("invalid %s arguments: %w", name, err)
		}
		if strings.TrimSpace(args.Query) == "" || len(args.Query) > 1024 || !utf8.ValidString(args.Query) {
			return nil, fmt.Errorf("invalid %s query", name)
		}
		matches, err := t.search(ctx, root, args.Path, args.Query, name == ToolReadDocs)
		if err != nil {
			return nil, err
		}
		result = map[string]any{"matches": matches, "truncated": len(matches) == t.limits.MaxMatches}
	case ToolReadFile:
		var args readArgs
		if err := decodeStrict(raw, &args); err != nil {
			return nil, fmt.Errorf("invalid read_file arguments: %w", err)
		}
		content, err := t.read(root, args)
		if err != nil {
			return nil, err
		}
		result = content
	default:
		return nil, fmt.Errorf("unknown agent tool %q", name)
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	if len(encoded) > t.limits.MaxResultBytes {
		return nil, errors.New("tool result exceeds configured byte limit")
	}
	return encoded, nil
}

func (t *Toolbox) search(ctx context.Context, root, requested, query string, docsOnly bool) ([]match, error) {
	start, err := t.resolve(root, requested, true)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(start)
	if err != nil {
		return nil, err
	}
	var files []string
	if info.IsDir() {
		err = filepath.WalkDir(start, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			if entry.Type()&os.ModeSymlink != 0 {
				if entry.IsDir() {
					return filepath.SkipDir
				}
				return nil
			}
			if entry.IsDir() {
				if path != start && excludedDirectory(entry.Name()) {
					return filepath.SkipDir
				}
				return nil
			}
			if t.allowedFile(root, path, docsOnly) {
				files = append(files, path)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	} else if t.allowedFile(root, start, docsOnly) {
		files = []string{start}
	} else {
		return nil, errors.New("requested path is not an allowed text file")
	}
	sort.Strings(files)
	needle := strings.ToLower(query)
	matches := make([]match, 0)
	for _, path := range files {
		data, err := t.readBytes(path)
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(bytes.NewReader(data))
		scanner.Buffer(make([]byte, 64<<10), int(t.limits.MaxFileBytes))
		line := 0
		for scanner.Scan() {
			line++
			if strings.Contains(strings.ToLower(scanner.Text()), needle) {
				relative, _ := filepath.Rel(root, path)
				matches = append(matches, match{Path: filepath.ToSlash(relative), Line: line, Text: truncateUTF8(scanner.Text(), 512)})
				if len(matches) >= t.limits.MaxMatches {
					return matches, nil
				}
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	return matches, nil
}

func (t *Toolbox) read(root string, args readArgs) (map[string]any, error) {
	path, err := t.resolve(root, args.Path, false)
	if err != nil {
		return nil, err
	}
	if !t.allowedFile(root, path, false) {
		return nil, errors.New("requested path is not an allowed text file")
	}
	data, err := t.readBytes(path)
	if err != nil {
		return nil, err
	}
	start := args.StartLine
	if start == 0 {
		start = 1
	}
	end := args.EndLine
	if end == 0 {
		end = start + 199
	}
	if start < 1 || end < start || end-start >= 500 {
		return nil, errors.New("invalid read_file line range")
	}
	lines := strings.Split(string(data), "\n")
	if start > len(lines) {
		return nil, errors.New("start_line is beyond end of file")
	}
	if end > len(lines) {
		end = len(lines)
	}
	relative, _ := filepath.Rel(root, path)
	return map[string]any{"path": filepath.ToSlash(relative), "start_line": start, "end_line": end, "content": strings.Join(lines[start-1:end], "\n"), "truncated": end < len(lines)}, nil
}

func (t *Toolbox) resolve(root, requested string, allowDirectory bool) (string, error) {
	if requested == "" && allowDirectory {
		return root, nil
	}
	if requested == "" || filepath.IsAbs(requested) || strings.ContainsRune(requested, '\x00') {
		return "", errors.New("path must be a non-empty repository-relative path")
	}
	clean := filepath.Clean(filepath.FromSlash(requested))
	if clean == ".." || strings.HasPrefix(clean, ".."+string(filepath.Separator)) {
		return "", errors.New("path escapes repository root")
	}
	joined := filepath.Join(root, clean)
	canonical, err := filepath.EvalSymlinks(joined)
	if err != nil {
		return "", fmt.Errorf("resolve requested path: %w", err)
	}
	relative, err := filepath.Rel(root, canonical)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", errors.New("path escapes repository root through symlink")
	}
	info, err := os.Stat(canonical)
	if err != nil {
		return "", err
	}
	if info.IsDir() != allowDirectory && info.IsDir() {
		return "", errors.New("requested path must be a file")
	}
	return canonical, nil
}

func (t *Toolbox) readBytes(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	data, err := io.ReadAll(io.LimitReader(file, t.limits.MaxFileBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > t.limits.MaxFileBytes {
		return nil, errors.New("file exceeds configured byte limit")
	}
	if bytes.IndexByte(data, 0) >= 0 || !utf8.Valid(data) {
		return nil, errors.New("binary or non-UTF-8 file is not allowed")
	}
	return data, nil
}

func (t *Toolbox) allowedFile(root, path string, docsOnly bool) bool {
	relative, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	parts := strings.Split(filepath.ToSlash(relative), "/")
	for _, part := range parts {
		if secretName(part) || excludedDirectory(part) {
			return false
		}
	}
	ext := strings.ToLower(filepath.Ext(path))
	allowed := map[string]bool{".go": true, ".py": true, ".rs": true, ".java": true, ".kt": true, ".c": true, ".h": true, ".cc": true, ".cpp": true, ".js": true, ".jsx": true, ".ts": true, ".tsx": true, ".sql": true, ".proto": true, ".yaml": true, ".yml": true, ".toml": true, ".json": true, ".md": true, ".txt": true, ".sh": true, ".xml": true}
	if !allowed[ext] {
		return false
	}
	if !docsOnly {
		return true
	}
	base := strings.ToLower(filepath.Base(path))
	return ext == ".md" || ext == ".txt" || strings.HasPrefix(base, "readme") || strings.Contains(filepath.ToSlash(relative), "docs/")
}

func excludedDirectory(name string) bool {
	switch strings.ToLower(name) {
	case ".git", ".hg", ".svn", "node_modules", "vendor", ".venv", "venv", "__pycache__":
		return true
	default:
		return false
	}
}

func secretName(name string) bool {
	lower := strings.ToLower(name)
	if lower == ".env" || strings.HasPrefix(lower, ".env.") || lower == "credentials" || lower == "credentials.json" || lower == "id_rsa" || lower == "id_ed25519" || lower == ".npmrc" || lower == ".pypirc" {
		return true
	}
	return strings.HasSuffix(lower, ".pem") || strings.HasSuffix(lower, ".key") || strings.HasSuffix(lower, ".p12") || strings.HasSuffix(lower, ".pfx")
}

func truncateUTF8(value string, maximum int) string {
	if len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

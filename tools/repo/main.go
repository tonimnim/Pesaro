// Command repo checks the scaffold's catalog and source ownership boundaries.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"go/format"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

type service struct {
	ID           string   `json:"id"`
	Name         string   `json:"name"`
	Port         int      `json:"port"`
	Stage        string   `json:"stage"`
	Phase        string   `json:"implementation_phase"`
	Dependencies []string `json:"planned_dependencies"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) != 1 || (args[0] != "check" && args[0] != "check-docs" && args[0] != "list") {
		return errors.New("usage from repository root: go run ./tools/repo <check|check-docs|list>")
	}
	checkDocs := args[0] == "check-docs"
	moduleFile, err := os.ReadFile("go.mod")
	if err != nil {
		return fmt.Errorf("run from repository root: %w", err)
	}
	module := ""
	for _, line := range strings.Split(string(moduleFile), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 2 && fields[0] == "module" {
			module = fields[1]
		}
	}
	if module == "" {
		return errors.New("go.mod has no module declaration")
	}
	data, err := os.ReadFile("services/catalog.json")
	if err != nil {
		return err
	}
	var catalog []service
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&catalog); err != nil {
		return err
	}
	if len(catalog) == 0 {
		return errors.New("empty service catalog")
	}
	if args[0] == "list" {
		for _, s := range catalog {
			fmt.Printf("%-20s 127.0.0.1:%d  %s\n", s.ID, s.Port, s.Stage)
		}
		return nil
	}
	var problems []string
	report := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	known := map[string]bool{}
	ports := map[int]string{}
	validID := regexp.MustCompile("^[a-z][a-z0-9-]*$")
	sections := []string{
		"## Purpose and ownership", "## Implementation status", "## Features",
		"## Planned contracts", "## Private data and events", "## Dependencies and trust",
		"## Failure and recovery", "## Acceptance criteria", "## Out of scope",
		"## Build and run", "## Delivery order",
	}
	for _, s := range catalog {
		if !validID.MatchString(s.ID) {
			report("invalid service ID %q", s.ID)
			continue
		}
		if known[s.ID] {
			report("duplicate service %s", s.ID)
		}
		known[s.ID] = true
		if s.Name == "" || s.Stage == "" || s.Phase == "" {
			report("%s: missing catalog metadata", s.ID)
		}
		if s.Port < 1024 || s.Port > 65535 {
			report("%s: invalid default port", s.ID)
		}
		if previous, ok := ports[s.Port]; ok {
			report("%s and %s share port %d", previous, s.ID, s.Port)
		}
		ports[s.Port] = s.ID
		required := []string{"cmd/" + s.ID + "/main.go", "internal/app/app.go"}
		if checkDocs {
			required = append(required, "AGENTS.md", "SPEC.md")
		}
		for _, name := range required {
			p := filepath.Join("services", s.ID, filepath.FromSlash(name))
			if info, err := os.Stat(p); err != nil || info.IsDir() {
				report("%s: required file missing", p)
			}
		}
		spec, err := os.ReadFile(filepath.Join("services", s.ID, "SPEC.md"))
		if checkDocs && err == nil {
			for _, section := range sections {
				if !strings.Contains(string(spec), section+"\n") && !strings.Contains(string(spec), section+"\r\n") {
					report("%s: missing specification section %q", s.ID, section)
				}
			}
		}
	}
	for _, s := range catalog {
		for _, dependency := range s.Dependencies {
			if !known[dependency] || dependency == s.ID {
				report("%s: invalid dependency %s", s.ID, dependency)
			}
		}
	}
	entries, err := os.ReadDir("services")
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() && !known[entry.Name()] {
			report("service directory absent from catalog: %s", entry.Name())
		}
	}
	markdownLink := regexp.MustCompile("]\\(([^ )]+)\\)")
	err = filepath.WalkDir(".", func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", ".cache", "out", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		extension := filepath.Ext(path)
		if extension != ".go" && !(checkDocs && extension == ".md") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if extension == ".md" {
			for _, match := range markdownLink.FindAllSubmatch(content, -1) {
				target := strings.Trim(string(match[1]), "<>")
				if strings.Contains(target, "://") || strings.HasPrefix(target, "mailto:") || strings.HasPrefix(target, "#") {
					continue
				}
				target = strings.SplitN(target, "#", 2)[0]
				if target == "" {
					continue
				}
				resolved := filepath.Join(filepath.Dir(path), filepath.FromSlash(target))
				if _, err := os.Stat(resolved); err != nil {
					report("%s: broken relative link %s", path, target)
				}
			}
			return nil
		}
		formatted, err := format.Source(content)
		if err != nil {
			report("%s: invalid Go: %v", path, err)
			return nil
		}
		if !bytes.Equal(content, formatted) {
			report("%s: run gofmt", path)
		}
		source, err := parser.ParseFile(token.NewFileSet(), path, content, parser.ImportsOnly)
		if err != nil {
			report("%s: %v", path, err)
			return nil
		}
		parts := strings.Split(filepath.ToSlash(path), "/")
		owner := ""
		if len(parts) >= 2 && parts[0] == "services" {
			owner = parts[1]
			if len(parts) < 4 || (parts[2] != "internal" && parts[2] != "cmd") {
				report("%s: service Go code must stay under internal or cmd", path)
			}
		}
		for _, imported := range source.Imports {
			value, err := strconv.Unquote(imported.Path.Value)
			if err != nil {
				report("%s: invalid import", path)
				continue
			}
			prefix := module + "/services/"
			if strings.HasPrefix(value, prefix) {
				target := strings.Split(strings.TrimPrefix(value, prefix), "/")[0]
				if owner == "" || target != owner {
					report("%s: forbidden service implementation import %s", path, value)
				}
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	if len(problems) != 0 {
		return errors.New(strings.Join(problems, "\n"))
	}
	checks := "catalog, source files, formatting and import boundaries"
	if checkDocs {
		checks += ", local specs and links"
	}
	fmt.Printf("Repository checks passed: %d services; %s.\n", len(catalog), checks)
	return nil
}

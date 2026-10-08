package celeris

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

var goBlock = regexp.MustCompile("(?s)```go\n(.*?)```")

// TestReadmeSnippetsCompile checks that every Go snippet in the README appears
// verbatim, give or take one level of indentation, in a file the build
// compiles, so the documentation cannot drift from the API.
func TestReadmeSnippetsCompile(t *testing.T) {
	readme, err := os.ReadFile("README.md")

	if err != nil {
		t.Fatal(err)
	}

	sources := []string{"example_test.go"}
	examples, err := filepath.Glob("examples/*/*.go")

	if err != nil {
		t.Fatal(err)
	}

	var compiled []string

	for _, path := range append(sources, examples...) {
		source, err := os.ReadFile(path)

		if err != nil {
			t.Fatal(err)
		}

		compiled = append(compiled, string(source), strings.ReplaceAll(string(source), "\n\t", "\n"))
	}

	blocks := goBlock.FindAllStringSubmatch(string(readme), -1)

	if len(blocks) == 0 {
		t.Fatal("the README has no Go snippets")
	}

	for _, block := range blocks {
		snippet := block[1]
		found := false

		for _, source := range compiled {
			if strings.Contains(source, snippet) {
				found = true

				break
			}
		}

		if !found {
			t.Errorf("README snippet is not in any compiled file:\n%s", snippet)
		}
	}
} // end function TestReadmeSnippetsCompile

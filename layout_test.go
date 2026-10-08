package celeris

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"
)

// TestSourceFollowsTheLayoutRules checks every Go file in the repository, the
// live suites and examples included, for the layout rules gofmt and wsl leave
// open (CONVENTIONS.md, Layout): a blank line between top-level declarations,
// a blank line after a statement that ends a func literal spanning lines, and
// an end marker after every function, method, struct and interface.
func TestSourceFollowsTheLayoutRules(t *testing.T) {
	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if entry.IsDir() && (entry.Name() == ".git" || entry.Name() == "testdata") {
			return filepath.SkipDir
		}

		if entry.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}

		for _, violation := range layoutViolations(t, path) {
			t.Error(violation)
		}

		return nil
	})

	if err != nil {
		t.Fatal(err)
	}
} // end function TestSourceFollowsTheLayoutRules

// layoutViolations parses one file and returns each violation as
// "file:line: rule".
func layoutViolations(t *testing.T, path string) []string {
	t.Helper()

	fileSet := token.NewFileSet()
	file, err := parser.ParseFile(fileSet, path, nil, parser.ParseComments)

	if err != nil {
		t.Fatal(err)
	}

	line := func(position token.Pos) int {
		return fileSet.Position(position).Line
	}

	var violations []string

	report := func(position token.Pos, rule string) {
		violations = append(violations, fmt.Sprintf("%s:%d: %s", path, line(position), rule))
	}

	// The comment that starts on each line, for markers and for what follows
	// a statement.
	commentOnLine := map[int]*ast.Comment{}

	for _, group := range file.Comments {
		for _, comment := range group.List {
			if _, seen := commentOnLine[line(comment.Slash)]; !seen {
				commentOnLine[line(comment.Slash)] = comment
			}
		}
	}

	expectMarker := func(opening, closing token.Pos, marker string) {
		if line(opening) == line(closing) {
			// A single-line struct{} or interface{} has no body to mark.
			if !strings.HasPrefix(marker, "// end function") && !strings.HasPrefix(marker, "// end method") {
				return
			}
		}

		comment := commentOnLine[line(closing)]

		if comment == nil || comment.Slash < closing || comment.Text != marker {
			report(closing, "end the block with "+marker)
		}
	}

	for index, declaration := range file.Decls {
		if index > 0 {
			previousEnd := line(file.Decls[index-1].End())
			start := declaration.Pos()

			if doc := declarationDoc(declaration); doc != nil {
				start = doc.Pos()
			}

			if line(start) <= previousEnd+1 {
				report(start, "leave a blank line between top-level declarations")
			}
		}

		switch declaration := declaration.(type) {
		case *ast.FuncDecl:
			if declaration.Body == nil {
				continue
			}

			kind := "function"

			if declaration.Recv != nil {
				kind = "method"
			}

			expectMarker(declaration.Body.Lbrace, declaration.Body.Rbrace, "// end "+kind+" "+declaration.Name.Name)
		case *ast.GenDecl:
			for _, spec := range declaration.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)

				if !ok {
					continue
				}

				switch typed := typeSpec.Type.(type) {
				case *ast.StructType:
					expectMarker(typed.Fields.Opening, typed.Fields.Closing, "// end struct "+typeSpec.Name.Name)
				case *ast.InterfaceType:
					expectMarker(typed.Methods.Opening, typed.Methods.Closing, "// end interface "+typeSpec.Name.Name)
				}
			}
		}
	}

	// wsl checks the blank line after block statements; a statement that ends
	// a func literal spanning lines is checked here.
	ast.Inspect(file, func(node ast.Node) bool {
		var statements []ast.Stmt

		switch node := node.(type) {
		case *ast.BlockStmt:
			statements = node.List
		case *ast.CaseClause:
			statements = node.Body
		case *ast.CommClause:
			statements = node.Body
		}

		for index := 0; index+1 < len(statements); index++ {
			statement := statements[index]
			end := line(statement.End())

			if !endsMultilineFuncLiteral(statement, end, line) {
				continue
			}

			next := line(statements[index+1].Pos())

			if comment := commentOnLine[end+1]; comment != nil {
				next = end + 1
			}

			if next <= end+1 {
				report(statement.End(), "leave a blank line after a func literal spanning lines")
			}
		}

		return true
	})

	return violations
} // end function layoutViolations

// declarationDoc returns the doc comment above a top-level declaration.
func declarationDoc(declaration ast.Decl) *ast.CommentGroup {
	switch declaration := declaration.(type) {
	case *ast.FuncDecl:
		return declaration.Doc
	case *ast.GenDecl:
		return declaration.Doc
	}

	return nil
} // end function declarationDoc

// endsMultilineFuncLiteral reports whether the statement's last line closes a
// func literal that spans lines.
func endsMultilineFuncLiteral(statement ast.Stmt, end int, line func(token.Pos) int) bool {
	found := false

	ast.Inspect(statement, func(node ast.Node) bool {
		literal, ok := node.(*ast.FuncLit)

		if ok && line(literal.Body.Rbrace) == end && line(literal.Body.Lbrace) < end {
			found = true
		}

		return !found
	})

	return found
} // end function endsMultilineFuncLiteral

package celeris

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The public surface, written down once. Every name traces to the
// specification's reference surface or a recorded decision.
var exportedSurface = []string{
	"const ErrBackpressure",
	"const ErrCancelled",
	"const ErrConfiguration",
	"const ErrDeliveryUnknown",
	"const ErrNotConnected",
	"const ErrOperationInProgress",
	"const ErrProtocol",
	"const ErrTimeout",
	"const ErrTransport",
	"const InternalError",
	"const MessageSizeLimitError",
	"const ParserError",
	"const PermissionDeniedError",
	"const RateLimitError",
	"const SendError",
	"const StateClosed",
	"const StateClosing",
	"const StateConnected",
	"const StateConnecting",
	"const StateFailed",
	"const StateIdle",
	"const StateReconnecting",
	"field ClientOptions.AllowInsecureLoopback",
	"field ClientOptions.BaseURL",
	"field ClientOptions.ConnectTimeout",
	"field ClientOptions.CredentialProvider",
	"field ClientOptions.PresenceQueryTimeout",
	"field CredentialRequest.ChannelReference",
	"field CredentialRequest.DisconnectedAt",
	"field CredentialRequest.Reconnect",
	"field CredentialRequest.ReplayLookback",
	"field Credentials.Payload",
	"field Credentials.Signature",
	"field Error.Code",
	"field Error.Field",
	"field Error.Message",
	"field Error.Offset",
	"field MessageMetadata.MessageID",
	"field MessageMetadata.SegmentID",
	"field MessageMetadata.Timestamp",
	"field MessageMetadata.TokenReference",
	"field PresenceConnection.ConnectionID",
	"field PresenceConnection.Timestamp",
	"field PresenceConnection.TokenReference",
	"field PresenceEvent.ConnectionID",
	"field PresenceEvent.Joined",
	"field PresenceEvent.SegmentID",
	"field PresenceEvent.Timestamp",
	"field PresenceEvent.TokenReference",
	"field PresencePage.Connections",
	"field PresencePage.CurrentPage",
	"field PresencePage.From",
	"field PresencePage.PerPage",
	"field PresencePage.SegmentID",
	"field PresencePage.To",
	"field PresencePage.Total",
	"field RecoveryEvent.PossibleDuplicates",
	"field RecoveryEvent.PossibleGaps",
	"field RecoveryEvent.RetryIndex",
	"field ServerError.Message",
	"field ServerError.Resource",
	"field ServerError.SubType",
	"field ServerError.Type",
	"field ServerNotice.Payload",
	"field ServerNotice.Timestamp",
	"func JSONPayload",
	"func NewClient",
	"func NewPayloadCodec",
	"func ReadJSON",
	"func ReadText",
	"func TextPayload",
	"method Channel.Close",
	"method Channel.Connect",
	"method Channel.DefaultSegment",
	"method Channel.Events",
	"method Channel.Segment",
	"method Channel.State",
	"method ChannelEventHandler.OnError",
	"method ChannelEventHandler.OnNotice",
	"method ChannelEventHandler.OnRecovery",
	"method ChannelEventHandler.OnStateChange",
	"method Client.Channel",
	"method Credentials.Format",
	"method Credentials.LogValue",
	"method Credentials.String",
	"method Error.Error",
	"method Error.Is",
	"method ErrorCode.Error",
	"method PayloadCodec.EncodePayload",
	"method PayloadCodec.ReadPayload",
	"method Segment.ID",
	"method Segment.OnMessage",
	"method Segment.OnPresence",
	"method Segment.PresenceList",
	"method Segment.Publish",
	"method Segment.PublishWithMessageID",
	"method Segment.Subscribe",
	"method Segment.SubscribePresence",
	"method ServerError.Error",
	"method Subscription.Cancel",
	"type Channel",
	"type ChannelEventHandler",
	"type ChannelState",
	"type Client",
	"type ClientOptions",
	"type CredentialProvider",
	"type CredentialRequest",
	"type Credentials",
	"type Error",
	"type ErrorCode",
	"type MessageMetadata",
	"type PayloadCodec",
	"type PresenceConnection",
	"type PresenceEvent",
	"type PresencePage",
	"type RecoveryEvent",
	"type Segment",
	"type ServerError",
	"type ServerErrorType",
	"type ServerNotice",
	"type Subscription",
}

// sourceFiles parses the module's non-test Go files: the package and its
// examples, everything a module download carries apart from tests.
func sourceFiles(t *testing.T) map[string]*ast.File {
	t.Helper()

	files := map[string]*ast.File{}
	fileSet := token.NewFileSet()

	err := filepath.WalkDir(".", func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// A nested module is not part of this one.
		if entry.IsDir() && path != "." {
			if _, err := os.Stat(filepath.Join(path, "go.mod")); err == nil {
				return filepath.SkipDir
			}
		}

		if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}

		file, err := parser.ParseFile(fileSet, path, nil, parser.ParseComments)
		files[path] = file

		return err
	})

	if err != nil {
		t.Fatal(err)
	}

	return files
}

func TestExportedSurfaceIsPinned(t *testing.T) {
	var surface []string

	for path, file := range sourceFiles(t) {
		if filepath.Dir(path) != "." {
			continue
		}

		for _, declaration := range file.Decls {
			surface = append(surface, exportedNames(declaration)...)
		}
	}

	slices.Sort(surface)

	if !slices.Equal(surface, exportedSurface) {
		t.Fatalf("exported surface changed:\n%s", strings.Join(surface, "\n"))
	}
}

func exportedNames(declaration ast.Decl) []string {
	var names []string

	switch declaration := declaration.(type) {
	case *ast.FuncDecl:
		if !declaration.Name.IsExported() {
			return nil
		}

		if declaration.Recv == nil {
			return []string{"func " + declaration.Name.Name}
		}

		receiver := declaration.Recv.List[0].Type

		if star, ok := receiver.(*ast.StarExpr); ok {
			receiver = star.X
		}

		if index, ok := receiver.(*ast.IndexExpr); ok {
			receiver = index.X
		}

		if receiverName := receiver.(*ast.Ident); receiverName.IsExported() {
			names = append(names, "method "+receiverName.Name+"."+declaration.Name.Name)
		}
	case *ast.GenDecl:
		for _, specification := range declaration.Specs {
			switch specification := specification.(type) {
			case *ast.TypeSpec:
				if !specification.Name.IsExported() {
					continue
				}

				names = append(names, "type "+specification.Name.Name)

				if structure, ok := specification.Type.(*ast.StructType); ok {
					for _, field := range structure.Fields.List {
						for _, fieldName := range field.Names {
							if fieldName.IsExported() {
								names = append(names, "field "+specification.Name.Name+"."+fieldName.Name)
							}
						}
					}
				}
			case *ast.ValueSpec:
				for _, valueName := range specification.Names {
					if valueName.IsExported() {
						names = append(names, declaration.Tok.String()+" "+valueName.Name)
					}
				}
			}
		}
	}

	return names
}

// AUTH-05: a client never signs, so nothing in this module can reach a
// signing facility. crypto/hmac and crypto/sha512 still appear among the
// transitive dependencies, through crypto/tls: they are TLS primitives, not a
// signing facility, so the check covers what this module imports itself and
// the module graph.
func TestNoSigningFacilityIsReachable(t *testing.T) {
	forbiddenImports := []string{"crypto/hmac", "crypto/sha256", "crypto/sha512", "github.com/useceleris/sdk-go-server"}

	for path, file := range sourceFiles(t) {
		for _, imported := range file.Imports {
			importPath, _ := strconv.Unquote(imported.Path.Value)

			if slices.Contains(forbiddenImports, importPath) {
				t.Errorf("%s imports %s", path, importPath)
			}
		}

		source, err := os.ReadFile(path)

		if err != nil {
			t.Fatal(err)
		}

		if strings.Contains(strings.ToLower(string(source)), "signingsecret") {
			t.Errorf("%s mentions a signing secret", path)
		}
	}

	for _, arguments := range [][]string{{"list", "-m", "all"}, {"list", "-deps", "./..."}} {
		command := exec.Command("go", arguments...)
		command.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=-mod=mod")
		output, err := command.CombinedOutput()

		if err != nil {
			t.Fatalf("go %s: %v\n%s", strings.Join(arguments, " "), err, output)
		}

		if strings.Contains(string(output), "sdk-go-server") {
			t.Errorf("go %s reaches the server module", strings.Join(arguments, " "))
		}
	}
}

// Credentials travel in the handshake URL, so the package prints and logs
// nothing; and importing it starts no work.
func TestPackageNeitherPrintsNorStartsWorkAtImport(t *testing.T) {
	for path, file := range sourceFiles(t) {
		if filepath.Dir(path) != "." {
			continue
		}

		ast.Inspect(file, func(node ast.Node) bool {
			switch node := node.(type) {
			case *ast.FuncDecl:
				if node.Recv == nil && node.Name.Name == "init" {
					t.Errorf("%s declares init", path)
				}
			case *ast.SelectorExpr:
				if packageName, ok := node.X.(*ast.Ident); ok {
					call := packageName.Name + "." + node.Sel.Name

					if packageName.Name == "log" || strings.HasPrefix(call, "fmt.Print") || strings.HasPrefix(call, "fmt.Fprint") || call == "os.Stdout" || call == "os.Stderr" || packageName.Name == "slog" && node.Sel.Name != "Value" && node.Sel.Name != "StringValue" {
						t.Errorf("%s uses %s", path, call)
					}
				}
			case *ast.Ident:
				if node.Name == "println" || node.Name == "print" {
					t.Errorf("%s uses %s", path, node.Name)
				}
			}

			return true
		})
	}
}

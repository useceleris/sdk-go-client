# Checks this module. Run from the repository root:
#
#   make check   formatting, vet, lint, vulnerabilities, tidiness and the unit
#                suites, with vet and lint on the live suites too
#   make live    the acceptance suites against a real Celeris stack, from .env
#   make fuzz    a minute of decoder fuzzing
#
# Releasing is pushing a version tag; nothing here does it.

GOLANGCI_LINT := github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0
GOVULNCHECK := golang.org/x/vuln/cmd/govulncheck@v1.8.0

.PHONY: check live fuzz

check:
	@test -z "$$(gofmt -l .)" || { gofmt -l .; echo "Run gofmt -w ."; exit 1; }
	go vet ./...
	go run $(GOLANGCI_LINT) run ./...
	go run $(GOVULNCHECK) ./...
	go mod tidy -diff
	go mod verify
	go test -race -count=1 ./...
	cd live && GOWORK=off go vet ./... && GOWORK=off go run $(GOLANGCI_LINT) run ./...

live:
	cd live && GOWORK=off go vet ./... && GOWORK=off go test -race -count=1 -timeout 30m ./...

fuzz:
	go test -run '^$$' -fuzz FuzzDecode -fuzztime 60s .

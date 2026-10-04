// The acceptance suites, against a real Celeris stack. A module of their own,
// so the test-only signer they need is not part of the client module.
module github.com/useceleris/sdk-go-client/live

go 1.27.0

require github.com/useceleris/sdk-go-client v0.0.0-00010101000000-000000000000

require github.com/coder/websocket v1.8.15 // indirect

replace github.com/useceleris/sdk-go-client => ../

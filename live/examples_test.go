package live

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"regexp"
	"testing"
	"time"
)

// TestRunsTheQuickstart runs examples/quickstart against an in-process
// credential endpoint that signs with this suite's test signer.
func TestRunsTheQuickstart(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			ChannelReference string `json:"channelReference"`
		}

		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || body.ChannelReference == "" {
			http.Error(writer, "invalid request", http.StatusBadRequest)

			return
		}

		// The quickstart publishes and receives on one connection.
		credentials := signCredentials(clientID(), signingSecret(), claim{"channel_references", []string{body.ChannelReference}}, claim{"allow_echo", true})
		_ = json.NewEncoder(writer).Encode(credentials)
	}))

	t.Cleanup(endpoint.Close)

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()

	command := exec.CommandContext(ctx, "go", "run", "./examples/quickstart")
	command.Dir = ".."
	command.Env = append(os.Environ(), "GOWORK=off", "CELERIS_CREDENTIAL_URL="+endpoint.URL, "CELERIS_SESSION=demo-session")
	output, err := command.CombinedOutput()

	if err != nil {
		t.Fatalf("quickstart failed: %v\n%s", err, output)
	}

	if !regexp.MustCompile(`example: ok delivered=[1-9]\d* present=\d+`).Match(output) {
		t.Fatalf("output:\n%s", output)
	}
}

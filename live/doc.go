// Package live holds the client's acceptance suites. Run them with make live
// from the repository root, with CELERIS_WS_URL, CELERIS_CLIENT_ID and
// CELERIS_SIGNING_SECRET in the root's .env or the environment. The optional
// CELERIS_WS_URL_PEER routes the cross-node tests' second connection to a
// different server node; without it, those tests skip.
package live

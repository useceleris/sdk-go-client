# Conventions

Simplicity and maintainability are paramount. These rules bind every change; the surface contract lives in the specifications repository, and the JavaScript client is the reference implementation this package mirrors.

## Descriptive names

Use full domain words: `releaseInterest`, `queueInterestSync`, `segmentID`, `credentialProvider`. No abbreviations, and no single-letter names outside tight loops. A name says what the thing is; a comment exists only to state a constraint the code cannot show.

## Simplicity over abstraction

Solve the problem in front of you with the simplest structure that stays readable. No registries, factories, event frameworks, dependency-injection containers or wrapper layers. A helper type earns its place only by removing real, present duplication (`listenerSet` qualifies; a "Manager" does not). Prefer a function over a type, and a method on an existing type over a new type.

Two seams exist, and no others should appear: the unexported `socket` interface with its `dialer`, so tests substitute an in-memory peer for the WebSocket, and `Client.random`, so tests fix the jitter. Time needs no seam: tests run in `testing/synctest` bubbles, whose clock is fake.

## Maintainability

- One package at the module root; unexported identifiers are its internals. Small files with one responsibility; the file name states it.
- Every fixed value lives in `constants.go`, as an unexported constant in Go's MixedCaps. Validation rules stay beside the code that uses them.
- Delete code in the same change that obsoletes it.
- Every exported identifier traces to a requirement or a recorded decision in the specifications repository (SEG-01, DEV-01, REV-01, ...). The surface is pinned in one list in `package_test.go`.
- Errors carry stable codes and messages that name what failed, where, and which rule or limit it broke. Never interpolate received values, input values, credentials or server text, and never wrap a cause: dial errors quote the credential URL.
- The channel's mutex guards all of its state and is never held while user code runs or while the socket blocks. Listeners are delivered through the event queue, never called directly.
- Tests are deterministic (`synctest` bubbles, fixed jitter), grouped by behaviour in topic files, and catch package-owned defects only. Golden vectors are hand-authored, never produced by the code under test.
- Before completion, review the full diff for anything deletable without weakening behaviour or tests.

## Layout

Leave one blank line after every block (`if`, `for`, `switch`, `select`, `func` literal spanning lines) before the next statement, except before `else` or at the end of an enclosing block, and one blank line between top-level declarations. Separate a declaration group from the block that uses it. Code packed against the block before it is harder to read, and is treated as a defect in review.

End every function, method, struct and interface with a marker that names it: `} // end function name`, `} // end method Name`, `} // end struct Name`, `} // end interface Name`. A single-line `struct{}` has no body and takes none.

The tools enforce this. `gofmt` owns layout; golangci-lint runs `wsl_v5` with only its `after-block` check, for the blank line after block statements; and `layout_test.go` parses every Go file in the repository, the live suites and examples included, for the rest: the blank line after a statement that ends a multi-line `func` literal, the blank line between top-level declarations, and the end markers. Each violation fails with its file and line. `make check` must pass.

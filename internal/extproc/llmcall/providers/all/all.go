// Package all registers every built-in llmcall provider via blank
// imports. It is the single shared touchpoint of the plugin design:
// adding a provider means adding its package and one import line here.
//
// The parsers shim imports this package, so any binary linking
// internal/extproc/parsers gets the full registry. Code that imports
// llmcall directly (tests included) must import this package itself or
// it will see an empty registry.
package all

import (
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/anthropic"
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/azureopenai"
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/bedrock"
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/google"
	_ "github.com/apoxy-dev/clrk/internal/extproc/llmcall/providers/openai"
)

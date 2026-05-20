// docs generates the Markdown reference for the clrk CLI. The output
// (`clrk.mdx`) is consumed by apoxy-cloud's docs2 site via
// `run/docs2/scripts/sync-clrk-cli.mjs`.
package main

import (
	"fmt"
	"os"

	"github.com/apoxy-dev/clrk/internal/cmd"
)

func main() {
	out := "./docs"
	if len(os.Args) > 1 {
		out = os.Args[1]
	}
	if err := cmd.GenerateDocs(out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

package main

import (
	"fmt"
	"os"

	"github.com/apoxy-dev/clrk/pkg/cmd"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

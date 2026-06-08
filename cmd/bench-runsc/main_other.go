//go:build !linux

package main

import (
	"fmt"
	"os"
)

func main() {
	fmt.Fprintln(os.Stderr, "bench-runsc: linux-only")
	os.Exit(1)
}

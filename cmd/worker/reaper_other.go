//go:build !linux

package main

// startChildReaper is a no-op on non-Linux. Worker only runs on Linux
// in production; the stub exists so go build ./... is green on darwin.
func startChildReaper() {}

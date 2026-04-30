package devtui

import "time"

// Status describes a component's lifecycle phase as rendered in the sidebar.
type Status int

const (
	StatusPending Status = iota
	StatusStarting
	StatusReady
	StatusError
)

// LogStream distinguishes container stdout from stderr (and from clrk's own
// orchestration log) for styling.
type LogStream int

const (
	StreamStdout LogStream = iota
	StreamStderr
	StreamClrk
)

// ComponentStatusMsg flips a component's status glyph in the sidebar.
type ComponentStatusMsg struct {
	Name   string
	Status Status
}

// LogLineMsg appends one line to a component's ring buffer.
type LogLineMsg struct {
	Source string
	Line   string
	Stream LogStream
}

// WatcherEvent enumerates rebuild lifecycle phases.
type WatcherEvent int

const (
	WatcherIdle WatcherEvent = iota
	WatcherBuilding
	WatcherReloaded
	WatcherFailed
)

// WatcherMsg surfaces rebuild progress in the sidebar's watcher block.
type WatcherMsg struct {
	Event    WatcherEvent
	Prefix   string
	Duration time.Duration
	Err      string
}

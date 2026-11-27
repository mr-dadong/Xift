package tray

import _ "embed"

// Dark32PNG is the 32x32 PNG tray icon suitable for light taskbars / macOS template tinting.
//
//go:embed xift-tray-dark-32.png
var Dark32PNG []byte

// Light32PNG is the 32x32 PNG tray icon suitable for dark taskbars.
//
//go:embed xift-tray-light-32.png
var Light32PNG []byte

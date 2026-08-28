package luabench

import _ "embed"

//go:embed product_merge.lua
var productMergeLua []byte

const benchmarkObservedAt = "2026-08-29T12:00:00Z"

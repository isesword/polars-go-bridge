//go:build !cgo
// +build !cgo

package bridge

// ArrowSchema represents Arrow schema in C (cgo disabled placeholder).
type ArrowSchema struct{}

// ArrowArray represents Arrow array data in C (cgo disabled placeholder).
type ArrowArray struct{}

// ReleaseArrowSchema is a no-op when cgo is disabled.
func ReleaseArrowSchema(_ *ArrowSchema) {}

// ReleaseArrowArray is a no-op when cgo is disabled.
func ReleaseArrowArray(_ *ArrowArray) {}

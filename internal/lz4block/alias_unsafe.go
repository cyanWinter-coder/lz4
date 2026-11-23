//go:build !nounsafe && !purego

package lz4block

import "unsafe"

// aliasCheckEnabled reports whether aliases can detect shared backing
// memory. It is false for the nounsafe/purego builds, which have no unsafe
// pointer comparison and therefore cannot identify overlapping slices.
const aliasCheckEnabled = true

// aliases reports whether the byte ranges underlying a and b share any
// memory. Empty slices never alias: they have no bytes to conflict over. The
// comparison uses the allocated regions, so sub-slices of one backing array
// are detected regardless of their position or capacity.
func aliases(a, b []byte) bool {
	if len(a) == 0 || len(b) == 0 {
		return false
	}
	pa := unsafe.Pointer(unsafe.SliceData(a))
	pb := unsafe.Pointer(unsafe.SliceData(b))
	// The byte span each slice covers starts at its data pointer and runs for
	// its length; two spans intersect iff neither ends before the other starts.
	const maxUintptr = ^uintptr(0)
	ea := uintptr(pa) + uintptr(len(a)) - 1
	eb := uintptr(pb) + uintptr(len(b)) - 1
	// Guard against pointer wrap on theoretical 32-bit extremes; real
	// allocations cannot span the whole address space, so this never fires.
	if ea < uintptr(pa) || eb < uintptr(pb) || ea == maxUintptr || eb == maxUintptr {
		return pa == pb
	}
	return uintptr(pa) <= eb && uintptr(pb) <= ea
}

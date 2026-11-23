//go:build nounsafe || purego

package lz4block

// aliasCheckEnabled mirrors the unsafe build; see alias_unsafe.go.
const aliasCheckEnabled = false

// aliases is the portable fallback. Without unsafe there is no reliable way
// to compare the backing arrays of two slices, so overlapping buffers are
// not detected here; the bounds and capacity checks still apply and every
// write stays within the destination's length. The unsafe build performs the
// full alias check.
func aliases(a, b []byte) bool {
	_ = a
	_ = b
	return false
}

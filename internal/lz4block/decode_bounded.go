package lz4block

import "github.com/pierrec/lz4/v4/internal/lz4errors"

const (
	// Return codes of the bounded decoder, mirroring decodeBlock's
	// negative-on-error convention.
	retInvalid = -1 // malformed block: bad offset, truncated token or trailing garbage
	retLimit   = -2 // the output budget (len(dst)) was reached
)

// maxVirtualOutput bounds the virtual output position tracked after the
// budget is full. A block cannot legitimately expand past this, so a larger
// position is a malformed block; the cap also prevents 32-bit position
// wraparound on hostile input.
const maxVirtualOutput = 1 << 30 // 1 GiB

// UncompressBlockBounded uncompresses src with dict into dst, treating
// len(dst) as the output budget, and reports which stop condition was hit.
//
// It splits the single error that UncompressBlock returns into two:
//   - a malformed block (zero offset, offset out of range, truncated token or
//     literal, trailing garbage) returns ErrInvalidSourceShortBuffer with
//     n == 0;
//   - a well-formed block that fills dst before its input is consumed returns
//     ErrOutputLimitExceeded with n == len(dst), where dst holds the exact
//     prefix of the unlimited output.
//
// A block that decodes completely within dst returns its size and a nil
// error. The decoder is portable Go on every platform, so its semantics are
// independent of the assembly decodeBlock kernels; callers that must not
// deliver any byte of a block whose tail is still unverified should run a
// full validation pass first (see BlockReader).
func UncompressBlockBounded(src, dst, dict []byte) (n int, err error) {
	if len(src) == 0 {
		return 0, nil
	}
	defer func() {
		if recover() != nil {
			// Every access is checked below, so a panic means malformed input
			// defeated a check rather than an over-budget block.
			n, err = 0, lz4errors.ErrInvalidSourceShortBuffer
		}
	}()
	switch r := decodeBlockBounded(dst, src, dict); r {
	case retInvalid:
		return 0, lz4errors.ErrInvalidSourceShortBuffer
	case retLimit:
		return len(dst), lz4errors.ErrOutputLimitExceeded
	default:
		return r, nil
	}
}

// decodeBlockBounded is decodeBlockGo with one difference: once the output
// budget (len(dst)) is exhausted it stops writing but keeps decoding to
// validate the rest of the block, tracking the virtual output position
// without storing bytes. It therefore reports:
//
//   - retInvalid if any part of the block is malformed, even if the budget
//     was already exhausted before the bad byte. Format errors are never
//     hidden by a budget;
//   - retLimit if the whole block is well formed but produces more than
//     len(dst) bytes, with dst holding the exact in-budget prefix;
//   - the uncompressed size otherwise.
//
// It never writes past len(dst).
func decodeBlockBounded(dst, src, dict []byte) (ret int) {
	limit := uint(len(dst))
	// Restrict capacities so a reslice cannot read or write out of bounds.
	dst = dst[:len(dst):len(dst)]
	src = src[:len(src):len(src)]

	var si, di uint
	// over is set once the virtual output position moves strictly past the
	// budget. A block that ends exactly at the budget is a success, so over
	// only flips when di exceeds limit. di keeps advancing as the virtual
	// position regardless; over only stops writes.
	over := false
	for si < uint(len(src)) {
		if di > maxVirtualOutput {
			// Hostile expansion past every plausible block: treat as malformed
			// rather than letting the virtual position wrap on 32-bit hosts.
			return retInvalid
		}
		// Literals and match lengths (token).
		b := uint(src[si])
		si++

		// Literals.
		if lLen := b >> 4; lLen > 0 {
			if lLen == 0xF {
				for {
					if si >= uint(len(src)) {
						return retInvalid
					}
					x := uint(src[si])
					if lLen += x; int(lLen) < 0 {
						return retInvalid
					}
					si++
					if x != 0xFF {
						break
					}
				}
			}
			if si+lLen > uint(len(src)) {
				return retInvalid
			}
			// di tracks the virtual position even past the budget; only the
			// part within the budget is written.
			if di < limit {
				copy(dst[di:], src[si:si+min(lLen, limit-di)])
			}
			di += lLen
			if di > limit {
				over = true
			}
			si += lLen
		}

		// Match.
		mLen := b & 0xF
		if si == uint(len(src)) && mLen == 0 {
			break
		} else if si+2 > uint(len(src)) {
			return retInvalid
		}
		mLen += minMatch

		offset := u16(src[si:])
		if offset == 0 {
			return retInvalid
		}
		si += 2

		if mLen >= 15+minMatch {
			for {
				if si >= uint(len(src)) {
					return retInvalid
				}
				x := uint(src[si])
				if mLen += x; int(mLen) < 0 {
					return retInvalid
				}
				si++
				if x != 0xFF {
					break
				}
			}
		}

		// Validate the match's source range. di is the virtual output
		// position and may be smaller than offset when the first part of the
		// match lives in the dictionary. Mirror decodeBlockGo: without a
		// dictionary, an offset past di is invalid.
		if di < offset {
			if len(dict) == 0 || offset-di > uint(len(dict)) {
				return retInvalid
			}
			// k bytes come from the dictionary before di reaches offset.
			fromDict := dict[uint(len(dict))+di-offset:]
			k := min(mLen, offset-di)
			if di < limit {
				copy(dst[di:], fromDict[:min(k, limit-di)])
			}
			di += k
			if mLen -= k; mLen == 0 {
				if di > limit {
					over = true
				}
				continue
			}
			// di == offset now; the rest copies from the block's own output.
		}

		// Walk the match one byte at a time so overlapping matches repeat the
		// window correctly. di stays the virtual position throughout; only
		// positions below the budget are written to dst.
		start := di - offset
		for mLen > 0 {
			if di < limit {
				dst[di] = dst[start]
			}
			di++
			start++
			mLen--
			if di > limit {
				over = true
			}
		}
	}

	if over {
		return retLimit
	}
	return int(di)
}

package lz4block

import (
	"encoding/binary"

	"github.com/pierrec/lz4/v4/internal/xxh32"
)

// dictIDVersion identifies the calculation below. Change it only if the
// meaning of a dictionary identity changes; frames and dictionaries that
// disagree on it then fail to match instead of decoding with the wrong
// bytes.
const dictIDVersion = 1

// DictID returns the identifier of dict that a frame records in its
// descriptor: a checksum over the identity-version, the length of the
// effective dictionary and its bytes. The effective dictionary is the last
// MaxDictSize bytes of dict, which is all that compression and decompression
// use, so dictionaries that share their tail have the same ID.
//
// Equal IDs mean equal effective dictionaries, so a frame made with one
// dictionary can be told apart from a frame made with another. It is not a
// cryptographic checksum. An empty dictionary has ID 0 and is not recorded.
func DictID(dict []byte) uint32 {
	if len(dict) > MaxDictSize {
		dict = dict[len(dict)-MaxDictSize:]
	}
	if len(dict) == 0 {
		return 0
	}
	var h xxh32.XXHZero
	var prefix [8]byte
	prefix[3] = dictIDVersion
	binary.LittleEndian.PutUint32(prefix[4:], uint32(len(dict)))
	_, _ = h.Write(prefix[:])
	_, _ = h.Write(dict)
	id := h.Sum32()
	if id == 0 {
		// 0 means that there is no dictionary, so a dictionary never has it.
		// The collision this introduces is as unlikely as any other.
		id = 1
	}
	return id
}

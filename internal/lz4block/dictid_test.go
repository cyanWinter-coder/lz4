package lz4block

import "testing"

func TestDictID(t *testing.T) {
	tail := testPattern(MaxDictSize, 1)
	long := append(testPattern(1000, 2), tail...)

	for _, tc := range []struct {
		name string
		a, b []byte
		same bool
	}{
		{"empty", nil, []byte{}, true},
		{"empty/nonempty", nil, []byte("x"), false},
		{"equal", []byte("abcdef"), []byte("abcdef"), true},
		{"oneByteDiffers", []byte("abcdef"), []byte("abcdeg"), false},
		{"prefixIgnored", long, tail, true},
		{"extraPrefixIgnored", tail, append([]byte("more"), tail...), true}, // longer than MaxDictSize
		{"tailDiffers", long, append(testPattern(1, 3), tail[:len(tail)-1]...), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := DictID(tc.a) == DictID(tc.b); got != tc.same {
				t.Fatalf("DictID(%d bytes)=%#x, DictID(%d bytes)=%#x: same=%v, want %v",
					len(tc.a), DictID(tc.a), len(tc.b), DictID(tc.b), got, tc.same)
			}
		})
	}
	if DictID(nil) != 0 || DictID([]byte{}) != 0 {
		t.Fatal("empty dictionary must have ID 0, which means no dictionary")
	}
	// The identity covers the dictionary length: a dictionary and a longer one
	// that shares its tail are equal by construction, but a different length of
	// suffix is a different dictionary.
	if DictID([]byte("abcd")) == DictID([]byte("abcde")) {
		t.Fatal("different lengths gave the same ID")
	}
}

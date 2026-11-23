package lz4errors

import "strconv"

type Error string

func (e Error) Error() string { return string(e) }

const (
	ErrInvalidSourceShortBuffer      Error = "lz4: invalid source or destination buffer too short"
	ErrInvalidFrame                  Error = "lz4: bad magic number"
	ErrInternalUnhandledState        Error = "lz4: unhandled state"
	ErrInvalidHeaderChecksum         Error = "lz4: invalid header checksum"
	ErrInvalidBlockChecksum          Error = "lz4: invalid block checksum"
	ErrInvalidFrameChecksum          Error = "lz4: invalid frame checksum"
	ErrOptionInvalidCompressionLevel Error = "lz4: invalid compression level"
	ErrOptionClosedOrError           Error = "lz4: cannot apply options on closed or in error object"
	ErrOptionInvalidBlockSize        Error = "lz4: invalid block size"
	ErrOptionNotApplicable           Error = "lz4: option not applicable"
	ErrWriterNotClosed               Error = "lz4: writer not closed"
	ErrWriterClosed                  Error = "lz4: writer closed"
	ErrEndOfStream                   Error = "lz4: end of stream reached"
	ErrInvalidFrameDescriptor        Error = "lz4: invalid or unsupported frame descriptor"
	ErrInvalidContentSize            Error = "lz4: content size mismatch"
	// ErrOverlappingBuffers is returned when a block call's source and
	// destination (or dictionary and destination) byte slices share memory.
	ErrOverlappingBuffers Error = "lz4: overlapping source and destination buffers"
)

// ShortBufferError reports that a destination buffer was too small. Need is
// the capacity that is guaranteed to be sufficient: CompressBlockBound of the
// input size for compression, or 0 when the required size is not known. The
// call writes nothing usable and returns a zero size with this error.
//
// A ShortBufferError matches ErrInvalidSourceShortBuffer through errors.Is,
// so code that handles the sentinel keeps working.
type ShortBufferError struct {
	// Have is the length of the destination buffer that was too small.
	Have int
	// Need is the guaranteed-sufficient destination length, or 0 if unknown.
	Need int
}

func (e *ShortBufferError) Error() string {
	if e.Need > 0 {
		return "lz4: destination buffer too short: have " + strconv.Itoa(e.Have) +
			" bytes, need " + strconv.Itoa(e.Need)
	}
	return "lz4: destination buffer too short: have " + strconv.Itoa(e.Have) + " bytes"
}

// Is keeps errors.Is(err, ErrInvalidSourceShortBuffer) true for the
// structured short-buffer error, preserving the sentinel-based contract.
func (e *ShortBufferError) Is(target error) bool {
	return target == ErrInvalidSourceShortBuffer
}

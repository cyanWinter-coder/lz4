package lz4errors

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
	// ErrDictionaryMismatch is returned when a frame records a dictionary
	// identity that differs from the Reader's dictionary.
	ErrDictionaryMismatch Error = "lz4: dictionary mismatch"
	// ErrDictionaryRequired is returned when a frame records a dictionary
	// that the Reader was not given.
	ErrDictionaryRequired Error = "lz4: frame requires a dictionary"
)

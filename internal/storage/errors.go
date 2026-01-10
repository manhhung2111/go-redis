package storage

// ErrorCode represents the type of storage error
type ErrorCode int

const (
	ErrNone ErrorCode = iota
	ErrWrongType
	ErrKeyNotFound
)

// StorageError represents a typed error from the storage layer
type StorageError struct {
	Code    ErrorCode
	Message string
}

func (e *StorageError) Error() string {
	return e.Message
}

var (
	ErrWrongTypeError   = &StorageError{Code: ErrWrongType, Message: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	ErrKeyNotFoundError = &StorageError{Code: ErrKeyNotFound, Message: "ERR no such key"}
)

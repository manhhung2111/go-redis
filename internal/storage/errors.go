package storage

// ErrorCode represents the type of storage error
type ErrorCode int

const (
	ErrNone ErrorCode = iota
	ErrWrongType
	ErrKeyNotFound
	ErrValueIsNotIntegerOrOutOfRange
	ErrValueIsNotValidFloat
	ErrCmSKeyAlreadyExists
	ErrCmSKeyDoesNotExist
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
	ErrWrongTypeError                     = &StorageError{Code: ErrWrongType, Message: "WRONGTYPE Operation against a key holding the wrong kind of value"}
	ErrKeyNotFoundError                   = &StorageError{Code: ErrKeyNotFound, Message: "ERR no such key"}
	ErrValueIsNotIntegerOrOutOfRangeError = &StorageError{Code: ErrValueIsNotIntegerOrOutOfRange, Message: "value is not an integer or out of range"}
	ErrValueIsNotValidFloatError          = &StorageError{Code: ErrValueIsNotValidFloat, Message: "value is not a valid float"}
	ErrCmSKeyAlreadyExistsError           = &StorageError{Code: ErrCmSKeyAlreadyExists, Message: "CMS: key already exists"}
	ErrCmSKeyDoesNotExistError            = &StorageError{Code: ErrCmSKeyDoesNotExist, Message: "CMS: key does not exist"}
)

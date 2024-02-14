package storage

import "errors"

var (
	DTOErrExample = NewErrDTO("example error")
	ErrNoRow      = NewErrDTO("can't find such row")
)

type ErrDTO struct {
	Err error
}

func NewErrDTO(msg string) error { return &ErrDTO{Err: errors.New(msg)} }

func (e *ErrDTO) Error() string { return e.Err.Error() }

package utils

import (
	"errors"
	"fmt"
)

var (
	ErrInvalidInput = errors.New("invalid input")
	ErrConnection   = errors.New("connection error")
	ErrCluster      = errors.New("cluster error")
	ErrStartup      = errors.New("startup error")
)

func PrintError(err error) {
	fmt.Printf("ERROR: %v\n", err)
}

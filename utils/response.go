package utils

import (
	"fmt"

	"github.com/bytechan/resp3"
)

func ExpectStringResponse(reader *resp3.Reader, expected string) error {
	value, _, err := reader.ReadValue()
	if err != nil {
		return err
	}

	result := value.SmartResult()
	switch response := result.(type) {
	case string:
		if response != expected {
			return fmt.Errorf("unexpected response: %s", response)
		}
	case error:
		return response
	default:
		return fmt.Errorf("unexpected response: %#v", result)
	}
	return nil
}

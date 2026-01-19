package errors

import (
	"fmt"
)

func InvalidNumberOfArgs(command string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", command)
}

func InvalidCommandOption(option, command string) error {
	return fmt.Errorf("ERR option '%s' is unsupported for '%s' command", option, command)
}

func InvalidCommand(command string) error {
	return fmt.Errorf("ERR command '%s' is unsupported", command)
}

func InvalidExpireTime(command string) error {
	return fmt.Errorf("ERR invalid expire time in '%s' command", command)
}

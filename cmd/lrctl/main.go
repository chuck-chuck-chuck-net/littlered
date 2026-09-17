package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/chuck-chuck-chuck-net/littlered/cmd/lrctl/cmd"
)

func main() {
	// Support for kubectl completion protocol.
	// When kubectl calls 'kubectl_complete-lr <args>', we must behave
	// like 'lrctl __complete <args>'.
	if filepath.Base(os.Args[0]) == "kubectl_complete-lr" {
		// Replace the command name with the hidden completion command
		newArgs := append([]string{"lrctl", "__complete"}, os.Args[1:]...)
		os.Args = newArgs
	}

	if err := cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

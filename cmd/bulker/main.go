package main

import (
	"bulker/internal/cmd/bulker"
	"log"
	"os"
)

func main() {
	var bulk *bulker.Bulker
	var err error
	if len(os.Args) < 2 {
		usage()
	}
	var cfile = os.Args[1]
	if bulk, err = bulker.New(cfile); err != nil {
		log.Printf("bulker main: unable to create bulker object: %s", err)
		os.Exit(1)
	}

	bulk.Run()
} // main

func usage() {
	log.Printf("Usage: bulker <path/to/config/file>")
	os.Exit(1)
}

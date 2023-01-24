// package logmanager performs log file actions (open. rotate)
package logmanager

import (
	"log"
	"os"
)

// OpenLog - open log file for writing
func OpenLog(logfile string) (*os.File, error) {
	// лог
	logfh, err := os.OpenFile(logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Fatalf("logmanager OpenLog: Error open log file %s: %+v", logfile, err)
		return nil, err
	}

	log.Printf("logmanager OpenLog: opening logfile")
	log.SetOutput(logfh)

	return logfh, nil
}

// RotateLog - rotate log file (close and reopen)
func RotateLog(logfh *os.File, logfile string) bool {
	log.Printf("logmanager RotateLog: rotating %s", logfile)

	var err error
	if err = logfh.Close(); err != nil {
		log.Printf("logmanager RotateLog: unable to close logfile %s: %s", logfile, err)
		//		return false
	}

	logfh, err = os.OpenFile(logfile, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.SetOutput(os.Stderr)
		log.Printf("logmanager RotateLog: unable to open log file %s: %s", logfile, err)
		return false
	}
	log.SetOutput(logfh)
	log.Printf("logmanager RotateLog: reopened logfile")
	return true
}

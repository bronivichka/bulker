package main

import (
	"bulker/internal/config"
	"bulker/internal/rabbitmq"
	"log"
	"os"
)

type cfg struct {
	RabbitMQ rabbitmq.RabbitMQConfig
}

var conf = new(cfg)

func main() {
	if len(os.Args) < 2 {
		usage()
	}
	var cfile = os.Args[1]

	log.Printf("Started with config %s", cfile)

	if err := config.ReadConfig(cfile, conf); err != nil {
		log.Printf("Unable to read config file: %v", err)
		os.Exit(1)
	}
	rmq := rabbitmq.New(conf.RabbitMQ)
	if err := rmq.Connect(); err != nil {
		log.Printf("Got error from RMQ Connect: %v", err)
		os.Exit(1)
	}
	if err := rmq.Init(); err != nil {
		log.Printf("Got error %v from Init", err)
	}

	log.Printf("Done")
}

func usage() {
	log.Printf("Usage: rmqinit <path/to/config/file>")
	os.Exit(1)
}

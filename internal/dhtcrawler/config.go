package dhtcrawler

import (
	"time"
)

type Config struct {
	ScalingFactor                uint
	BootstrapNodes               []string
	ReseedBootstrapNodesInterval time.Duration
	SaveFilesThreshold           uint
	SavePieces                   bool
	RescrapeThreshold            time.Duration
}

func NewDefaultConfig() Config {
	return Config{
		ScalingFactor:                10,
		BootstrapNodes:               defaultBootstrapNodes,
		ReseedBootstrapNodesInterval: time.Minute,
		SaveFilesThreshold:           100,
		SavePieces:                   false,
		RescrapeThreshold:            time.Hour * 24 * 30,
	}
}

var defaultBootstrapNodes = []string{
	"dht.libtorrent.org:25401",
	"dht.transmissionbt.com:6881",
	"router.bittorrent.com:6881",
	"router.utorrent.com:6881",
	"dht.aelitis.com:6881",
}

package main

import (
	"log"
	"net"

	"github.com/adrgs/proiectchord/chord"
)

func GetOutboundIP() string {
	conn, err := net.Dial("udp", "127.0.0.1:53")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

func main() {

	node, err := chord.NewChordNode(GetOutboundIP())

	if err != nil {
		log.Fatalf("Couldn't start node")
	}

	node.Join()
}

package main

import (
	"fmt"
	"log"
	"net"

	"github.com/adrgs/proiectchord/chord"
	"github.com/fatih/color"
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

func info(node *chord.ChordNode) {
	bold := color.New(color.Bold).SprintFunc()
	fmt.Printf("%s: %s\n", bold("IP"), node.Ip)
	fmt.Printf("%s: %d\n", bold("Id"), node.Id)
	fmt.Printf("%s: %d\n", bold("M bits"), chord.CHORD_M)
	fmt.Printf("%s: %d\n", bold("Successor id"), node.Successor.Id)
}

func main() {

	node, err := chord.NewChordNode(GetOutboundIP())

	if err != nil {
		log.Fatalf("Couldn't start node")
	}

	node.Join()

	info(node)
}

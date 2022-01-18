package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"

	"github.com/adrgs/proiectchord/chord"
	"github.com/fatih/color"
)

func GetOutboundIP() string {
	conn, err := net.Dial("udp", "1.1.1.1:53")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	return localAddr.IP.String()
}

var cyan = color.New(color.FgCyan).PrintfFunc()
var hostname, _ = os.Hostname()
var bold = color.New(color.Bold).SprintFunc()

func info(node *chord.ChordNode) {
	node.SuccessorMutex.RLock()
	defer node.SuccessorMutex.RUnlock()

	node.PredecessorMutex.RLock()
	defer node.PredecessorMutex.RUnlock()

	node.FingerTableMutex.RLock()
	defer node.FingerTableMutex.RUnlock()

	cyan("\n== Nod %s ==\n", hostname)

	fmt.Printf("%s: %s\n", bold("IP"), node.Ip)
	fmt.Printf("%s: %d\n", bold("Id"), node.Id)
	fmt.Printf("%s: %d\n", bold("M bits"), chord.CHORD_M)
	fmt.Printf("%s: %d (%s)\n", bold("Successor id"), node.Successor.Id, node.Successor.Ip)
	fmt.Printf("%s: %d (%s)\n", bold("Predecessor id"), node.Predecessor.Id, node.Predecessor.Ip)

	fmt.Printf("%s:\n", bold("Finger Table"))
	fmt.Printf("\t%s %d (successor) -> %d (%s)\n", bold("Finger"), 0, node.FingerTable[0].Id, node.FingerTable[0].Ip)
	for i := 1; i < chord.CHORD_M; i++ {
		fmt.Printf("\t%s %d -> %d (%s)\n", bold("Finger"), i, node.FingerTable[i].Id, node.FingerTable[i].Ip)
	}
	fmt.Println()
}

func viz_tabel(node *chord.ChordNode) {
	cyan("\n== Nod %s ==\n", hostname)
	fmt.Printf("%s:\n", bold("Hash Table"))
	for key, value := range node.HashTable {
		fmt.Printf("\t%s (%d) -> %s\n", bold(key), chord.ChordHash([]byte(key)), value)
	}
	fmt.Println()
}

func string_stdin() string {
	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		return scanner.Text()
	}
	return ""
}

func meniu() {
	println("1. Info nod")
	println("2. Vizualizare tabel de dispersie nod")
	println("3. Interogare DHT")
	println("4. Adauga valoare in DHT")
	println("5. Iesire nod")
}

func getDHT(node *chord.ChordNode) {
	fmt.Printf("\nKey=")
	key := string_stdin()

	val, ok := node.GetChord(key)

	if ok {
		fmt.Printf("Value found=%v\n", val)
	} else {
		fmt.Printf("Value not found\n\n")
	}
}

func storeDHT(node *chord.ChordNode) {
	fmt.Printf("\nKey=")
	key := string_stdin()
	fmt.Printf("Value=")
	value := string_stdin()

	err := node.StoreChord(key, value)

	if err == nil {
		fmt.Println("Value written to DHT\n")
	} else {
		fmt.Println("Value could not be written to DHT\n")
	}
}

func main() {

	node, err := chord.NewChordNode(GetOutboundIP())

	if err != nil {
		log.Fatalf("Couldn't start node")
	}

	node.Join()

	var choice int
	for {
		meniu()
		fmt.Printf("Optiune=")
		_, err := fmt.Scanf("%d\n", &choice)
		if err != nil || choice > 4 || choice < 1 {
			node.Leave()
			fmt.Println("La revedere!")
			os.Exit(0)
		}
		switch choice {
		case 1:
			info(node)
		case 2:
			viz_tabel(node)
		case 3:
			getDHT(node)
		case 4:
			storeDHT(node)
		}
	}
}

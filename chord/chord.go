package chord

import (
	"crypto/sha1"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"github.com/adrgs/proiectchord/chordpb"
	"github.com/hashicorp/consul/api"
)

func get_m() int {
	m := os.Getenv("CHORD_M")
	i, err := strconv.Atoi(m)
	if err != nil || len(m) == 0 {
		return 12
	}
	return i
}

var CHORD_M = get_m()

func ChordHash(data []byte) int {
	h := sha1.New()
	h.Write(data)
	hash := h.Sum(nil)

	var rezultat int = 0
	biti := get_m()

	for bit := 0; bit < biti; bit += 1 {
		idx := len(hash) - 1 - (bit / 8)
		rezultat += int(((hash[idx] >> (bit % 8)) & 1)) << bit
	}

	return rezultat
}

type ChordNode struct {
	*chordpb.Node

	Successor      *chordpb.Node
	SuccessorMutex sync.RWMutex

	Predecessor      *chordpb.Node
	PredecessorMutex sync.RWMutex

	FingerTable      []*chordpb.Node
	FingerTableMutex sync.RWMutex

	HashTable      map[string]string
	HashTableMutex sync.RWMutex

	GrpcServer *grpc.Server
}

func NewChordNode(ip string) (*ChordNode, error) {

	chordNode := &ChordNode{
		Node:             &chordpb.Node{},
		Successor:        nil,
		SuccessorMutex:   sync.RWMutex{},
		Predecessor:      nil,
		PredecessorMutex: sync.RWMutex{},
		FingerTable:      make([]*chordpb.Node, CHORD_M),
		FingerTableMutex: sync.RWMutex{},
		HashTable:        map[string]string{},
	}

	chordNode.Ip = ip
	chordNode.Id = int64(ChordHash([]byte(ip)))

	/*chordNode.Successor = chordNode.Node
	chordNode.Predecessor = chordNode.Node
	chordNode.FingerTable[0] = chordNode.Node*/

	chordNode.GrpcServer = grpc.NewServer()

	addr := fmt.Sprintf("%s:13337", chordNode.Ip)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	chordpb.RegisterChordServiceServer(chordNode.GrpcServer, chordNode)
	reflection.Register(chordNode.GrpcServer)

	go chordNode.GrpcServer.Serve(lis)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				chordNode.stabilize()
				chordNode.fixFingers()
			}
		}
	}()

	return chordNode, nil
}

func (chordNode *ChordNode) lookup(key string) string {
	keyHash := ChordHash([]byte(key))
	successor := chordNode.findSuccessor(int64(keyHash))
	if successor == nil {
		return ""
	}
	return successor.Ip
}

func (chordNode *ChordNode) findSuccessor(id int64) *chordpb.Node {
	predecessor := chordNode.findPredecessor(id)

	if predecessor == nil {
		return nil
	}

	chordServiceClient, conn := chordNode.connectRemote(predecessor.Ip)

	defer conn.Close()

	successor, err := chordServiceClient.GetSuccessor(context.Background(), &chordpb.Nil{})
	if err != nil {
		return nil
	}

	return successor
}

func between(id int64, low int64, high int64) bool {
	if low < high {
		if id > low && id < high {
			return true
		}
	}
	if low > high {
		if id > low || id < high {
			return true
		}
	}
	if low == high {
		return id != low
	}
	return false
}

func (chordNode *ChordNode) findPredecessor(id int64) *chordpb.Node {

	n := chordNode.closestPrecedingFinger(id)

	if n.Id == chordNode.Id {
		return n
	}

	chordServiceClient, conn := chordNode.connectRemote(n.Ip)
	nSuccessor, err := chordServiceClient.GetSuccessor(context.Background(), &chordpb.Nil{})
	conn.Close()

	if err != nil {
		return n
	}

	for !(between(id, n.Id, nSuccessor.Id) || id == nSuccessor.Id) {
		// fmt.Println(id, n.Id, nSuccessor.Id)

		if n.Id == nSuccessor.Id {
			return n
		}

		chordServiceClient, conn := chordNode.connectRemote(n.Ip)
		n, err := chordServiceClient.ClosestPrecedingFinger(context.Background(), &chordpb.Id{Id: id})
		conn.Close()

		if err != nil || n == nil {
			return nil
		}

		chordServiceClient, conn = chordNode.connectRemote(n.Ip)
		nSuccessor, err = chordServiceClient.GetSuccessor(context.Background(), &chordpb.Nil{})
		conn.Close()

		if err != nil || nSuccessor == nil {
			return nil
		}
	}

	return n
}

func (chordNode *ChordNode) closestPrecedingFinger(id int64) *chordpb.Node {

	// // fmt.Println("ClosestPrecedingFinger called")
	chordNode.FingerTableMutex.RLock()
	defer chordNode.FingerTableMutex.RUnlock()

	for i := CHORD_M - 1; i >= 0; i -= 1 {
		if chordNode.FingerTable[i] != nil {
			if between(chordNode.FingerTable[i].Id, chordNode.Id, id) {
				return chordNode.FingerTable[i]
			}
		}
	}

	return chordNode.Node
}

var Client *api.Client

func (chordNode *ChordNode) Leave() {
	Client.Agent().ServiceDeregister(chordNode.Ip)

	successor := chordNode.getSuccessor()
	predecessor := chordNode.getPredecessor()

	if successor.Id != chordNode.Id && predecessor != nil {

		chordServiceClient, conn := chordNode.connectRemote(successor.Ip)
		defer conn.Close()
		chordServiceClient.SetPredecessor(context.Background(), predecessor)

		chordServiceClient2, conn2 := chordNode.connectRemote(successor.Ip)
		defer conn2.Close()
		chordServiceClient2.SetSuccessor(context.Background(), successor)
	}

	chordNode.GrpcServer.GracefulStop()
}

func (chordNode *ChordNode) Join() error {
	config := api.DefaultConfig()
	config.Address = "consul:8500"
	Client, _ = api.NewClient(config)

	agent := Client.Agent()

	reg := &api.AgentServiceRegistration{
		ID:      chordNode.Ip,
		Name:    "chord",
		Port:    13337,
		Address: chordNode.Ip,
		Check: &api.AgentServiceCheck{
			TCP:      fmt.Sprintf("%s:13337", chordNode.Ip),
			Timeout:  "1s",
			Interval: "3s",
		},
	}

	err := agent.ServiceRegister(reg)

	addrs, _, err := Client.Health().Service("chord", "", true, &api.QueryOptions{RequireConsistent: true})

	if err != nil {
		return err
	}

	// fmt.Println("Len addrs:", len(addrs))

	for _, addr := range addrs {
		if chordNode.Ip != addr.Service.Address {
			chordNode.initFingerTable(addr.Service.Address)
			chordNode.updateOthers()
			return nil
		}
	}

	chordNode.setPredecessor(chordNode.Node)
	chordNode.setSuccessor(chordNode.Node)
	for i := 1; i < CHORD_M; i++ {
		chordNode.setFingerTable(i, chordNode.Node)
	}

	return nil
}

func (chordNode *ChordNode) initFingerTable(ip string) {
	chordServiceClient, conn := chordNode.connectRemote(ip)
	defer conn.Close()
	// fmt.Println("chordService", chordServiceClient, ip, chordNode.Ip)

	successor, err := chordServiceClient.FindSuccessor(context.Background(), &chordpb.Id{Id: chordNode.Id})
	if err != nil {
		return
	}

	precSuccessor, err := chordServiceClient.FindPredecessor(context.Background(), &chordpb.Id{Id: successor.Id})

	if err != nil {
		return
	}
	// fmt.Println("successor", successor)
	// fmt.Println("precSuccessor", precSuccessor)

	if successor.Id == chordNode.Id || err != nil {
		log.Fatalf("ID already in ring (collision): %v", successor.Id)
	}

	chordNode.setPredecessor(precSuccessor)
	chordNode.setSuccessor(successor)

	chordServiceClient.SetPredecessor(context.Background(), chordNode.Node)
	if successor.Id == precSuccessor.Id {
		chordServiceClient.SetSuccessor(context.Background(), chordNode.Node)
	}

	// fmt.Println("Before for")

	for i := 1; i < CHORD_M; i++ {
		start := chordNode.fingerStart(i)
		// fmt.Println("for:", start, chordNode.Id, chordNode.getFingerTable(i-1).Id)
		if between(start, chordNode.Id, chordNode.getFingerTable(i-1).Id) || start == chordNode.Id {
			// fmt.Println("Path1")
			chordNode.setFingerTable(i, chordNode.getFingerTable(i-1))
		} else {
			// fmt.Println("Path2")
			finger, _ := chordServiceClient.FindSuccessor(context.Background(), &chordpb.Id{Id: start})
			// fmt.Println("Finger", finger)
			chordNode.setFingerTable(i, finger)
		}
	}
	// fmt.Println("InitFingerTable ended")
}

func (chordNode *ChordNode) updateOthers() {
	// fmt.Println("UpdateOthers called")
	for i := 0; i < CHORD_M; i++ {
		p := chordNode.findPredecessor(chordNode.fingerEnd(i))
		chordServiceClient, conn := chordNode.connectRemote(p.Ip)
		chordServiceClient.UpdateFingerTable(context.Background(), &chordpb.UFTRequest{Node: chordNode.Node, Idx: int64(i)})
		conn.Close()
	}
	// fmt.Println("UpdateOthers ended")
}

func (chordNode *ChordNode) updateFingerTable(uftRequest *chordpb.UFTRequest) {
	node := uftRequest.Node
	i := uftRequest.Idx
	// fmt.Println("UpdateFingerTable", node.Id, chordNode.Id, chordNode.getFingerTable(int(i)).Id)
	if between(node.Id, chordNode.Id, chordNode.getFingerTable(int(i)).Id) || node.Id == chordNode.Id {
		// fmt.Println("Inside if")
		chordNode.setFingerTable(int(i), node)
		p := chordNode.getPredecessor()
		chordServiceClient, conn := chordNode.connectRemote(p.Ip)
		chordServiceClient.UpdateFingerTable(context.Background(), uftRequest)
		conn.Close()
	}
}

func (chordNode *ChordNode) stabilize() {
	successor := chordNode.getSuccessor()

	if successor == nil {
		return
	}

	chordServiceClient, conn := chordNode.connectRemote(successor.Ip)
	x, err := chordServiceClient.GetPredecessor(context.Background(), &chordpb.Nil{})
	conn.Close()

	if err != nil {
		return
	}

	if x == nil && err == nil {
		chordServiceClient, conn = chordNode.connectRemote(successor.Ip)
		chordServiceClient.Notify(context.Background(), chordNode.Node)
		conn.Close()

		return
	}

	if between(x.Id, chordNode.Id, successor.Id) {
		chordNode.setSuccessor(x)
	}

	chordServiceClient, conn = chordNode.connectRemote(successor.Ip)
	chordServiceClient.Notify(context.Background(), chordNode.Node)
	conn.Close()
}

func (chordNode *ChordNode) notify(node *chordpb.Node) error {
	chordNode.PredecessorMutex.Lock()
	defer chordNode.PredecessorMutex.Unlock()

	if chordNode.Predecessor == nil || between(node.Id, chordNode.Predecessor.Id, chordNode.Id) {
		oldPredecessor := chordNode.Predecessor
		chordNode.Predecessor = node
		if oldPredecessor != nil {
			chordNode.transferKeys(chordNode.Predecessor, oldPredecessor.Id, chordNode.Predecessor.Id)
		}
	}

	return nil
}

func (chordNode *ChordNode) fixFingers() {
	i := rand.Int() % CHORD_M
	start := chordNode.fingerStart(i)

	finger := chordNode.findSuccessor(start)
	if finger != nil {
		chordNode.setFingerTable(i, finger)
	}
}

func (chordNode *ChordNode) fingerStart(i int) int64 {
	max := int64(math.Pow(2, float64(CHORD_M)))
	val := chordNode.Id + int64(math.Pow(2, float64(i)))

	return val % max
}

func (chordNode *ChordNode) FingerStart(i int) int64 {
	max := int64(math.Pow(2, float64(CHORD_M)))
	val := chordNode.Id + int64(math.Pow(2, float64(i)))

	return val % max
}

func (chordNode *ChordNode) fingerEnd(i int) int64 {
	max := int64(math.Pow(2, float64(CHORD_M)))
	val := chordNode.Id - int64(math.Pow(2, float64(i)))

	return (val + max) % max
}

func (chordNode *ChordNode) transferKeys(node *chordpb.Node, start int64, end int64) {
	chordNode.HashTableMutex.Lock()
	defer chordNode.HashTableMutex.Unlock()

	for key, value := range chordNode.HashTable {
		keyHash := int64(ChordHash([]byte(key)))
		if between(keyHash, start, end) {

			chordServiceClient, conn := chordNode.connectRemote(node.Ip)
			chordServiceClient.Store(context.Background(), &chordpb.StoreRequest{Key: key, Value: value})
			conn.Close()

			delete(chordNode.HashTable, key)
		}
	}
}

func (chordNode *ChordNode) getSuccessor() *chordpb.Node {
	chordNode.SuccessorMutex.RLock()
	defer chordNode.SuccessorMutex.RUnlock()
	return chordNode.Successor
}

func (chordNode *ChordNode) getPredecessor() *chordpb.Node {
	chordNode.PredecessorMutex.RLock()
	defer chordNode.PredecessorMutex.RUnlock()
	return chordNode.Predecessor
}

func (chordNode *ChordNode) setSuccessor(successorNode *chordpb.Node) {
	chordNode.SuccessorMutex.Lock()
	chordNode.Successor = successorNode
	chordNode.SuccessorMutex.Unlock()
	chordNode.FingerTableMutex.Lock()
	chordNode.FingerTable[0] = successorNode
	chordNode.FingerTableMutex.Unlock()
}

func (chordNode *ChordNode) setPredecessor(predecessorNode *chordpb.Node) {
	chordNode.PredecessorMutex.Lock()
	chordNode.Predecessor = predecessorNode
	chordNode.PredecessorMutex.Unlock()
}

func (chordNode *ChordNode) setFingerTable(i int, node *chordpb.Node) {
	chordNode.FingerTableMutex.Lock()
	chordNode.FingerTable[i] = node
	chordNode.FingerTableMutex.Unlock()
	if i == 0 {
		chordNode.SuccessorMutex.Lock()
		chordNode.Successor = node
		chordNode.SuccessorMutex.Unlock()
	}
}

func (chordNode *ChordNode) getFingerTable(i int) *chordpb.Node {
	chordNode.FingerTableMutex.RLock()
	defer chordNode.FingerTableMutex.RUnlock()
	return chordNode.FingerTable[i]
}

func (chordNode *ChordNode) get(key string) (string, bool) {
	ip := chordNode.lookup(key)
	if ip == chordNode.Ip {
		chordNode.HashTableMutex.RLock()
		defer chordNode.HashTableMutex.RUnlock()
		val, ok := chordNode.HashTable[key]
		return val, ok
	}

	// fmt.Println("Ip", ip, &chordpb.GetRequest{Key: key})

	chordServiceClient, conn := chordNode.connectRemote(ip)
	defer conn.Close()
	val, err := chordServiceClient.Get(context.Background(), &chordpb.GetRequest{Key: key})
	if err != nil {
		return "", false
	}

	return val.Value, err == nil
}

func (chordNode *ChordNode) GetChord(key string) (string, bool) {
	return chordNode.get(key)
}

func (chordNode *ChordNode) StoreChord(key string, value string) error {
	return chordNode.store(key, value)
}

func (chordNode *ChordNode) store(key string, value string) error {
	ip := chordNode.lookup(key)
	if ip == chordNode.Ip {
		chordNode.HashTableMutex.Lock()
		chordNode.HashTable[key] = value
		chordNode.HashTableMutex.Unlock()
		return nil
	}

	chordServiceClient, conn := chordNode.connectRemote(ip)
	defer conn.Close()
	_, err := chordServiceClient.Store(context.Background(), &chordpb.StoreRequest{Key: key, Value: value})

	return err
}

func (chordNode *ChordNode) getFile(path string) ([]byte, bool) {
	// fmt.Println("get File")
	return []byte(path), true
}

func (chordNode *ChordNode) storeFile(path string, data []byte) error {
	// fmt.Println("store File")
	return nil
}

func (chordNode *ChordNode) connectRemote(ip string) (chordpb.ChordServiceClient, *grpc.ClientConn) {
	addr := fmt.Sprintf("%s:13337", ip)
	conn, err := grpc.Dial(addr, grpc.WithInsecure())

	if err != nil {
		log.Printf("Unable to connect to %s: %v", addr, err)
		return nil, nil
	}

	chordServiceClient := chordpb.NewChordServiceClient(conn)

	return chordServiceClient, conn
}

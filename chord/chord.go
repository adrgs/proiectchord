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
	"strings"
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
		return 12 // default value
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
		idx := len(hash) - 1 - (bit % 8)
		rezultat |= int(((hash[idx] >> bit) & 1) << bit)
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

	chordNode.GrpcServer = grpc.NewServer()

	addr := fmt.Sprintf("%s:13337", chordNode.Ip)

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	chordpb.RegisterChordServiceServer(chordNode.GrpcServer, chordNode)
	reflection.Register(chordNode.GrpcServer)

	go chordNode.GrpcServer.Serve(lis)

	if err = chordNode.join(); err != nil {
		log.Fatalf("Failed to join ring: %v", err)
	}

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

func (chordNode *ChordNode) findPredecessor(id int64) *chordpb.Node {
	n := chordNode.closestPrecedingFinger(id)
	if n.Id == chordNode.Id {
		return n
	}

	chordServiceClient, conn := chordNode.connectRemote(n.Ip)
	nSuccessor, err := chordServiceClient.GetSuccessor(context.Background(), &chordpb.Nil{})
	conn.Close()

	if err != nil || nSuccessor == nil {
		return nil
	}

	for !(id > n.Id && id <= nSuccessor.Id) {
		chordServiceClient, conn = chordNode.connectRemote(n.Ip)
		n, err = chordServiceClient.ClosestPrecedingFinger(context.Background(), &chordpb.Id{Id: id})
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

	chordNode.FingerTableMutex.RLock()
	defer chordNode.FingerTableMutex.RUnlock()

	for i := CHORD_M - 1; i >= 0; i -= 1 {
		if chordNode.FingerTable[i] != nil {
			if chordNode.FingerTable[i].Id > chordNode.Id && chordNode.FingerTable[i].Id < id {
				return chordNode.FingerTable[i]
			}
		}
	}

	return chordNode.Node
}

func (chordNode *ChordNode) join() error {
	config := api.DefaultConfig()
	config.Address = "consul:8500"
	consul, err := api.NewClient(config)
	if err != nil {
		return err
	}

	nodes, _, err := consul.Catalog().Nodes(&api.QueryOptions{})
	if err != nil {
		return err
	}
	if len(nodes) == 0 {
		chordNode.setPredecessor(chordNode.Node)
		chordNode.setSuccessor(chordNode.Node)
		for i := 1; i < CHORD_M; i++ {
			chordNode.setFingerTable(i, chordNode.Node)
		}
	}

	ip := strings.Split(nodes[0].Address, ":")[0]

	chordNode.initFingerTable(ip)
	chordNode.updateOthers()

	return nil
}

func (chordNode *ChordNode) initFingerTable(ip string) {
	chordServiceClient, conn := chordNode.connectRemote(ip)
	defer conn.Close()

	successor, err := chordServiceClient.FindSuccessor(context.Background(), &chordpb.Id{Id: chordNode.Id})
	precSuccessor, err := chordServiceClient.FindPredecessor(context.Background(), &chordpb.Id{Id: successor.Id})

	if successor.Id == chordNode.Id || err != nil {
		log.Fatalf("ID already in ring (collision): %v", successor.Id)
	}

	chordNode.setPredecessor(precSuccessor)
	chordNode.setSuccessor(successor)

	chordServiceClient.SetSuccessor(context.Background(), chordNode.Node)

	for i := 1; i < CHORD_M; i++ {
		start := chordNode.fingerStart(i)
		if start >= chordNode.Id && start < chordNode.getFingerTable(i-1).Id {
			chordNode.setFingerTable(i, chordNode.getFingerTable(i-1))
		} else {
			finger, _ := chordServiceClient.FindSuccessor(context.Background(), &chordpb.Id{Id: start})
			chordNode.setFingerTable(i, finger)
		}
	}
}

func (chordNode *ChordNode) updateOthers() {
	for i := 0; i < CHORD_M; i++ {
		p := chordNode.findPredecessor(chordNode.fingerEnd(i))
		chordServiceClient, conn := chordNode.connectRemote(p.Ip)
		chordServiceClient.UpdateFingerTable(context.Background(), &chordpb.UFTRequest{Node: chordNode.Node, Idx: int64(i)})
		conn.Close()
	}
}

func (chordNode *ChordNode) updateFingerTable(uftRequest *chordpb.UFTRequest) {
	node := uftRequest.Node
	i := uftRequest.Idx
	if node.Id >= chordNode.Id && node.Id < chordNode.getFingerTable(int(i)).Id {
		chordNode.setFingerTable(int(i), node)
		p := chordNode.getPredecessor()
		chordServiceClient, conn := chordNode.connectRemote(p.Ip)
		chordServiceClient.UpdateFingerTable(context.Background(), uftRequest)
		conn.Close()
	}
}

func (chordNode *ChordNode) stabilize() {
	successor := chordNode.getSuccessor()
	chordServiceClient, conn := chordNode.connectRemote(successor.Ip)
	x, err := chordServiceClient.GetPredecessor(context.Background(), &chordpb.Nil{})
	conn.Close()

	if x == nil && err == nil {
		chordServiceClient, conn = chordNode.connectRemote(successor.Ip)
		chordServiceClient.Notify(context.Background(), chordNode.Node)
		conn.Close()

		return
	}

	if x.Id > chordNode.Id && x.Id < successor.Id {
		chordNode.setSuccessor(x)
	}

	chordServiceClient, conn = chordNode.connectRemote(successor.Ip)
	chordServiceClient.Notify(context.Background(), chordNode.Node)
	conn.Close()
}

func (chordNode *ChordNode) notify(node *chordpb.Node) error {
	chordNode.PredecessorMutex.Lock()
	defer chordNode.PredecessorMutex.Unlock()

	if chordNode.Predecessor == nil || (node.Id > chordNode.Predecessor.Id && node.Id < chordNode.Id) {
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
		if keyHash > start && keyHash <= end {

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
	chordNode.setFingerTable(0, successorNode)
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
		chordNode.setSuccessor(node)
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

	chordServiceClient, conn := chordNode.connectRemote(ip)
	defer conn.Close()
	val, err := chordServiceClient.Get(context.Background(), &chordpb.GetRequest{Key: key})

	return val.Value, err == nil
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
	fmt.Println("get File")
	return []byte(path), true
}

func (chordNode *ChordNode) storeFile(path string, data []byte) error {
	fmt.Println("store File")
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

/*

func (chordNode *ChordNode) registerService() error {
	config := api.DefaultConfig()
	config.Address = node.SDAddress
	consul, err := api.NewClient(config)
	if err != nil {
		log.Println("Unable to contact Service Discovery:", err)
		return err
	}

	kv := consul.KV()
	pair := &api.KVPair{Key: KeyPrefix + node.Name, Value: []byte(node.Addr)}
	_, err = kv.Put(pair, nil)
	if err != nil {
		log.Println("Unable to register with Service Discovery:", err)
		return err
	}

	// store the kv for future use
	node.SDKV = *kv

	log.Println("Successfully registered with Consul.")
	return nil
}

func (chordNode *ChordNode) Start() error {
	node.Peers = make(map[string]pb.ChordServiceClient)

	go node.StartListening()

	if err := node.registerService(); err != nil {
		return err
	}

	for {
		// TODO: pull messages from centralized channel
		// and broadcast those to all known peers
		// let just sleep for now
		node.BroadcastMessage("Hello from " + node.Name)
		time.Sleep(time.Minute * 1)
	}
}

func (chordNode *ChordNode) BroadcastMessage(message string) {
	// get all nodes -- inefficient, but this is just an example
	kvpairs, _, err := node.SDKV.List(KeyPrefix, nil)
	if err != nil {
		log.Println("Error getting keypairs from service discovery:", err)
		return
	}

	for _, kventry := range kvpairs {
		if strings.Compare(kventry.Key, KeyPrefix+node.Name) == 0 {
			// ourself
			continue
		}
		if node.Peers[kventry.Key] == nil {
			fmt.Println("New member: ", kventry.Key)
			// connection not established previously
			node.SetupClient(kventry.Key, string(kventry.Value))
		}
	}
}

func (chordNode *ChordNode) SetupClient(name string, addr string) {
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Printf("Unable to connect to %s: %v", addr, err)
	}

	defer conn.Close()

	node.Peers[name] = chordpb.NewChordServiceClient(conn)

	response, err := node.Peers[name].SayHello(context.Background(), &pb.ChordRequest{Name: node.Name})
	if err != nil {
		log.Printf("Error making request to %s: %v", name, err)
	}

	log.Printf("Greeting from other node: %s", response.getMessage())
}
*/

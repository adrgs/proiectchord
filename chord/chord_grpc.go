package chord

import (
	"context"
	"errors"

	"github.com/adrgs/proiectchord/chordpb"
)

func (chordNode *ChordNode) FindSuccessor(ctx context.Context, id *chordpb.Id) (*chordpb.Node, error) {
	successor := chordNode.findSuccessor(id.Id)

	return successor, nil
}

func (chordNode *ChordNode) FindPredecessor(ctx context.Context, id *chordpb.Id) (*chordpb.Node, error) {
	successor := chordNode.findPredecessor(id.Id)

	return successor, nil
}

func (chordNode *ChordNode) ClosestPrecedingFinger(ctx context.Context, id *chordpb.Id) (*chordpb.Node, error) {
	cpf := chordNode.closestPrecedingFinger(id.Id)

	return cpf, nil
}

func (chordNode *ChordNode) Notify(ctx context.Context, node *chordpb.Node) (*chordpb.Nil, error) {
	chordNode.notify(node)

	return &chordpb.Nil{}, nil
}

func (chordNode *ChordNode) UpdateFingerTable(ctx context.Context, uftRequest *chordpb.UFTRequest) (*chordpb.Nil, error) {
	chordNode.updateFingerTable(uftRequest)

	return &chordpb.Nil{}, nil
}

func (chordNode *ChordNode) GetSuccessor(ctx context.Context, n *chordpb.Nil) (*chordpb.Node, error) {
	node := chordNode.getSuccessor()

	return node, nil
}

func (chordNode *ChordNode) GetPredecessor(ctx context.Context, n *chordpb.Nil) (*chordpb.Node, error) {
	node := chordNode.getPredecessor()

	return node, nil
}

func (chordNode *ChordNode) SetSuccessor(ctx context.Context, node *chordpb.Node) (*chordpb.Nil, error) {
	chordNode.setSuccessor(node)

	return &chordpb.Nil{}, nil
}

func (chordNode *ChordNode) SetPredecessor(ctx context.Context, node *chordpb.Node) (*chordpb.Nil, error) {
	chordNode.setPredecessor(node)

	return &chordpb.Nil{}, nil
}

func (chordNode *ChordNode) Get(ctx context.Context, getRequest *chordpb.GetRequest) (*chordpb.GetReply, error) {
	val, ok := chordNode.get(getRequest.Key)

	if ok {
		return &chordpb.GetReply{Value: val}, nil
	}

	return &chordpb.GetReply{Value: ""}, errors.New("No value found")
}

func (chordNode *ChordNode) Store(ctx context.Context, storeRequest *chordpb.StoreRequest) (*chordpb.Nil, error) {
	chordNode.store(storeRequest.Key, storeRequest.Value)

	return &chordpb.Nil{}, nil
}

func (chordNode *ChordNode) GetFile(ctx context.Context, getRequest *chordpb.GetFileRequest) (*chordpb.GetFileReply, error) {
	data, ok := chordNode.getFile(getRequest.Path)

	if ok {
		return &chordpb.GetFileReply{Data: data}, nil
	}

	return nil, nil
}

func (chordNode *ChordNode) StoreFile(ctx context.Context, storeRequest *chordpb.StoreFileRequest) (*chordpb.Nil, error) {
	chordNode.storeFile(storeRequest.Path, storeRequest.Data)

	return &chordpb.Nil{}, nil
}

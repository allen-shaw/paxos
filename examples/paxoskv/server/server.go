package main

import (
	"context"
	"github.com/AllenShaw19/paxos/examples/paxoskv"
	"github.com/AllenShaw19/paxos/examples/paxoskv/proto"
	"github.com/AllenShaw19/paxos/paxos"
)

type KvServer struct {
	*proto.UnimplementedPaxosKVServerServer
	kv *paxoskv.PaxosKV
}

func NewKvServer(node *paxos.NodeInfo,
	nodes paxos.NodeInfoList,
	kvDBPath, paxosLogPath string) *KvServer {

}

func (s *KvServer) Init() error {

}

func (s *KvServer) Put(ctx context.Context, request *proto.KVOperator) (*proto.KVResponse, error) {

}

func (s *KvServer) GetLocal(ctx context.Context, request *proto.KVOperator) (*proto.KVResponse, error) {

}

func (s *KvServer) GetGlobal(ctx context.Context, request *proto.KVOperator) (*proto.KVResponse, error) {

}

func (s *KvServer) Delete(ctx context.Context, request *proto.KVOperator) (*proto.KVResponse, error) {

}

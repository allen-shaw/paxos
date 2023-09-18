package main

import (
	"context"
	"github.com/AllenShaw19/paxos/examples/paxoskv"
	pb "github.com/AllenShaw19/paxos/examples/paxoskv/proto"
	"github.com/AllenShaw19/paxos/log"
	"github.com/AllenShaw19/paxos/paxos"
)

type KvServer struct {
	*pb.UnimplementedPaxosKVServerServer
	kv *paxoskv.PaxosKV
}

func NewKvServer(node *paxos.NodeInfo,
	nodes paxos.NodeInfoList,
	kvDBPath, paxosLogPath string) *KvServer {
	kv := paxoskv.NewPaxosKV(node, nodes, kvDBPath, paxosLogPath)
	return &KvServer{kv: kv}
}

func (s *KvServer) Init() error {
	return s.kv.RunPaxos()
}

func (s *KvServer) Put(ctx context.Context, request *pb.KVOperator) (*pb.KVResponse, error) {
	reply := &pb.KVResponse{}
	if !s.kv.IsMaster(request.Key) {
		masterNodeID := s.kv.GetMaster(request.Key).GetNodeId()
		reply.Ret = paxoskv.KVStatusNoMaster
		reply.MasterNodeid = uint64(masterNodeID)

		log.Info("i'm not master, need redirect",
			log.Uint64("master.nodeid", uint64(masterNodeID)),
			log.String("key", request.Key),
			log.Uint64("version", request.Version))

		return reply, paxoskv.ErrKVStatusRedirect
	}

	err := s.kv.Put(request.Key, string(request.Value), request.Version)
	if err != nil {
		log.Error("kv.put fail", log.Err(err),
			log.String("key", request.Key),
			log.Uint64("version", request.Version))
		return nil, err
	}
	return reply, nil
}

func (s *KvServer) GetLocal(ctx context.Context, request *pb.KVOperator) (*pb.KVResponse, error) {
	value, version, err := s.kv.GetLocal(request.Key)
	if err != nil && err != paxoskv.ErrKVStatusKeyNotExist {
		log.Error("kv.get local fail", log.Err(err),
			log.String("key", request.Key))
		return nil, err
	}

	reply := &pb.KVResponse{Data: &pb.KVData{}}
	if err == paxoskv.ErrKVStatusKeyNotExist {
		reply.Data.IsDeleted = true
		reply.Data.Version = version
		return reply, paxoskv.ErrKVStatusKeyNotExist
	}

	reply.Data.IsDeleted = false
	reply.Data.Value = []byte(value)
	reply.Data.Version = version
	return reply, nil
}

func (s *KvServer) GetGlobal(ctx context.Context, request *pb.KVOperator) (*pb.KVResponse, error) {
	reply := &pb.KVResponse{}
	if !s.kv.IsMaster(request.Key) {
		masterNodeID := s.kv.GetMaster(request.Key).GetNodeId()
		reply.Ret = paxoskv.KVStatusNoMaster
		reply.MasterNodeid = uint64(masterNodeID)

		log.Info("i'm not master, need redirect",
			log.Uint64("master.nodeid", uint64(masterNodeID)),
			log.String("key", request.Key),
			log.Uint64("version", request.Version))

		return reply, paxoskv.ErrKVStatusRedirect
	}
	return s.GetLocal(ctx, request)
}

func (s *KvServer) Delete(ctx context.Context, request *pb.KVOperator) (*pb.KVResponse, error) {
	reply := &pb.KVResponse{}
	if !s.kv.IsMaster(request.Key) {
		masterNodeID := s.kv.GetMaster(request.Key).GetNodeId()
		reply.Ret = paxoskv.KVStatusNoMaster
		reply.MasterNodeid = uint64(masterNodeID)

		log.Info("i'm not master, need redirect",
			log.Uint64("master.nodeid", uint64(masterNodeID)),
			log.String("key", request.Key),
			log.Uint64("version", request.Version))

		return reply, paxoskv.ErrKVStatusRedirect
	}
	err := s.kv.Delete(request.Key, request.Version)
	if err != nil {
		log.Error("kv.delete fail", log.Err(err),
			log.String("key", request.Key),
			log.Uint64("version", request.Version))
		return nil, err
	}
	return reply, nil
}

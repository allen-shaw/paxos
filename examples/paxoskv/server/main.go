package main

import (
	"errors"
	"flag"
	"fmt"
	pb "github.com/AllenShaw19/paxos/examples/paxoskv/proto"
	"github.com/AllenShaw19/paxos/paxos"
	"github.com/AllenShaw19/paxos/plugin/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
	"net"
	"strconv"
	"strings"
)

// ./phxkv_grpcserver
//		--grpc=127.0.0.1:21112
//		--paxos=127.0.0.1:11112
//		--cluster=127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113
//		--kvdb=./storage/kvdb_1
//		--paxoslog=./storage/paxoslog_1
func main() {
	serverAddr := flag.String("grpc", "127.0.0.1:21112", "grpc ip:port")
	paxosAddr := flag.String("paxos", "127.0.0.1:11112", "paxos ip:port")
	nodeInfoList := flag.String("cluster", "127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113", "node0_ip:node0_port,node1_ip:node1_port,node2_ip:node2_port,...>")
	kvDBPath := flag.String("kvdb", "./storage/kvdb_1", "kv db path")
	paxosLogPath := flag.String("paxoslog", "./storage/paxoslog_1", "paxos log path")

	flag.Parse()
	nodeInfo, err := parseIpPort(paxosAddr)
	if err != nil {
		fmt.Printf("parse paxos ip:port fail, err: %v\n", err)
		panic(err)
	}

	nodeInfos, err := parseIpPortList(nodeInfoList)
	if err != nil {
		fmt.Printf("parse ip/port list fail, err: %v\n", err)
		panic(err)
	}

	log.Info("server init start............................")

	kvServer := NewKvServer(nodeInfo, nodeInfos, *kvDBPath, *paxosLogPath)
	err = kvServer.Init()
	if err != nil {
		fmt.Printf("server init fail, err: %v\n", err)
		panic(err)
	}

	log.Info("server init ok ............................")

	lis, err := net.Listen("tcp", *serverAddr)
	if err != nil {
		fmt.Printf("failed to listen: %v", err)
		panic(err)
	}

	fmt.Printf("server listening on %s", *serverAddr)
	s := grpc.NewServer()
	pb.RegisterPaxosKVServerServer(s, kvServer)
	reflection.Register(s)
	err = s.Serve(lis)
	if err != nil {
		fmt.Printf("failed to serve: %v", err)
		panic(err)
	}
}

func parseIpPort(addr *string) (*paxos.NodeInfo, error) {
	ss := strings.Split(*addr, ":")
	if len(ss) != 2 {
		return nil, errors.New("invalid addr")
	}
	ip := ss[0]
	p := ss[1]

	port, err := strconv.Atoi(p)
	if err != nil {
		return nil, err
	}

	nodeInfo := &paxos.NodeInfo{}
	nodeInfo.SetIPPort(ip, port)

	return nodeInfo, nil
}

func parseIpPortList(addrs *string) (paxos.NodeInfoList, error) {
	ss := strings.Split(*addrs, ",")

	nodeInfos := make(paxos.NodeInfoList, 0)
	for _, addr := range ss {
		nodeInfo, err := parseIpPort(&addr)
		if err != nil {
			return nil, err
		}
		nodeInfos = append(nodeInfos, nodeInfo)
	}

	return nodeInfos, nil
}

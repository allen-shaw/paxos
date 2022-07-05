package paxoskv

import (
	"encoding/binary"
	pb "github.com/AllenShaw19/paxos/examples/paxoskv/proto"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/cockroachdb/pebble"
	"github.com/golang/protobuf/proto"
	"sync"
)

var (
	kvClient *KVClient
	once     sync.Once
)

type KVClient struct {
	pebbleDB *pebble.DB
	hasInit  bool
	mutex    sync.Mutex
}

func NewKVClient() *KVClient {
	return &KVClient{hasInit: false, pebbleDB: nil}
}

func Instance() *KVClient {
	if kvClient == nil {
		once.Do(func() {
			kvClient = NewKVClient()
		})
	}
	return kvClient
}

func (c *KVClient) Init(dbPath string) (err error) {
	if c.hasInit {
		return nil
	}
	c.pebbleDB, err = pebble.Open(dbPath, nil)
	if err != nil {
		log.Error("open pebble fail", log.Err(err), log.String("db_path", dbPath))
		return err
	}

	c.hasInit = true
	log.Info("client db init ok", log.String("db_path", dbPath))
	return nil
}

func (c *KVClient) Get(key string) (value string, version uint64, err error) {
	if !c.hasInit {
		log.Error("no init yet")
		return "", 0, ErrKvClientSysFail
	}

	buff, closer, err := c.pebbleDB.Get([]byte(key))
	defer closer.Close()
	if err != nil {
		if err == pebble.ErrNotFound {
			log.Error("pebbleDB.Get not found", log.String("key", key))
			return "", 0, ErrKvClientKeyNotExist
		}

		log.Error("pebbleDB.Get fail", log.Err(err), log.String("key", key))
		return "", 0, ErrKvClientSysFail
	}

	data := &pb.KVData{}
	err = proto.Unmarshal(buff, data)
	if err != nil {
		log.Error("db data wrong", log.String("key", key))
		return "", 0, ErrKvClientSysFail
	}

	version = data.Version
	if data.IsDeleted {
		log.Error("pebbleDB.Get key already deleted", log.String("key", key))
		return "", version, ErrKvClientKeyNotExist
	}

	value = string(data.Value)
	log.Info("ok", log.String("key", key), log.String("value", value), log.Uint64("version", version))
	return value, version, nil
}

func (c *KVClient) Set(key, value string, version uint64) (err error) {
	if !c.hasInit {
		log.Error("no init yet")
		return ErrKvClientSysFail
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, serverVersion, err := c.Get(key)
	if err != nil && err != ErrKvClientKeyNotExist {
		return ErrKvClientSysFail
	}

	if serverVersion != version {
		return ErrKvClientKeyVersionConflict
	}

	serverVersion++
	data := &pb.KVData{}
	data.Value = []byte(value)
	data.Version = version
	data.IsDeleted = false
	buff, err := proto.Marshal(data)
	if err != nil {
		log.Error("data marshal fail", log.Err(err))
		return ErrKvClientSysFail
	}

	err = c.pebbleDB.Set([]byte(key), buff, &pebble.WriteOptions{})
	if err != nil {
		log.Error("pebbleDB.Put fail", log.Err(err), log.String("key", key))
		return ErrKvClientSysFail
	}

	log.Info("ok", log.String("key", key), log.String("value", value), log.Uint64("version", version))
	return nil
}

func (c *KVClient) Del(key string, version uint64) error {
	if !c.hasInit {
		log.Error("no init yet")
		return ErrKvClientSysFail
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	serverValue, serverVersion, err := c.Get(key)
	if err != nil && err != ErrKvClientKeyNotExist {
		return ErrKvClientSysFail
	}

	if serverVersion != version {
		return ErrKvClientKeyVersionConflict
	}

	serverVersion++
	data := &pb.KVData{}
	data.Value = []byte(serverValue)
	data.Version = version
	data.IsDeleted = true
	buff, err := proto.Marshal(data)
	if err != nil {
		log.Error("data marshal fail", log.Err(err))
		return ErrKvClientSysFail
	}

	err = c.pebbleDB.Set([]byte(key), buff, &pebble.WriteOptions{})
	if err != nil {
		log.Error("pebbleDB.Put fail", log.Err(err), log.String("key", key))
		return ErrKvClientSysFail
	}

	log.Info("ok", log.String("key", key), log.Uint64("version", version))
	return nil
}

func (c *KVClient) GetCheckpointInstanceID() (checkpointInstanceID uint64, err error) {
	if !c.hasInit {
		log.Error("no init yet")
		return 0, ErrKvClientSysFail
	}

	key := make([]byte, 0)
	binary.BigEndian.PutUint64(key, KvCheckpointKey)

	buff, closer, err := c.pebbleDB.Get(key)
	defer closer.Close()
	if err != nil {
		if err == pebble.ErrNotFound {
			return 0, ErrKvClientKeyNotExist
		}
		return 0, ErrKvClientSysFail
	}
	checkpointInstanceID = binary.BigEndian.Uint64(buff)

	log.Info("ok", log.Uint64("checkpointInstanceID", checkpointInstanceID))

	return checkpointInstanceID, nil
}

func (c *KVClient) SetCheckpointInstanceID(checkpointInstanceID uint64) error {
	if !c.hasInit {
		log.Error("no init yet")
		return ErrKvClientSysFail
	}
	key := make([]byte, 0)
	binary.BigEndian.PutUint64(key, KvCheckpointKey)

	buff := make([]byte, 0)
	binary.BigEndian.PutUint64(buff, checkpointInstanceID)

	options := &pebble.WriteOptions{Sync: true}
	err := c.pebbleDB.Set(key, buff, options)
	if err != nil {
		log.Error("pebbleDB.Put fail", log.Err(err))
		return ErrKvClientSysFail
	}

	log.Info("ok", log.Uint64("checkpointInstanceID", checkpointInstanceID))

	return nil
}

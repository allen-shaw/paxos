package logstorage

import (
	"encoding/binary"
	"errors"
	"github.com/AllenShaw19/paxos/paxos/utils/bin"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/cockroachdb/pebble"
	"math"
)

const (
	minChosenKey       uint64 = math.MaxUint64
	systemVariablesKey uint64 = math.MaxUint64 - 1
	masterVariablesKey uint64 = math.MaxUint64 - 2
)

func newPaxosComparer() *pebble.Comparer {
	c := &pebble.Comparer{
		Name: "PaxosComparer",
		Compare: func(a, b []byte) int {
			if len(a) != bin.SizeOfUint64 {
				log.Error("invalid a.len", log.Int("a.len", len(a)))
				panic("a.len != sizeof uint64")
			}
			if len(b) != bin.SizeOfUint64 {
				log.Error("invalid b.len", log.Int("b.len", len(b)))
				panic("b.len != sizeof uint64")
			}

			i := binary.BigEndian.Uint64(a)
			j := binary.BigEndian.Uint64(b)

			if i == j {
				return 0
			}
			if i < j {
				return -1
			}
			return 1
		},
	}
	return c
}

type Database struct {
	pebbleDB *pebble.DB
	paxosCmp *pebble.Comparer
	inited   bool

	valueStore *LogStore
	dbPath     string

	groupIdx int
}

func NewDatabase() *Database {
	return &Database{
		inited:   false,
		groupIdx: -1,
		paxosCmp: newPaxosComparer(),
	}
}

func (db *Database) Close() {
	if db.pebbleDB != nil {
		err := db.pebbleDB.Close()
		if err != nil {
			log.Error("pebbleDB")
		}
		db.pebbleDB = nil
	}
	log.Info("pebbleDB deleted.", log.String("path", db.dbPath))
}

func (db *Database) Init(dbPath string, groupIdx int) error {
	if db.inited {
		return nil
	}
	db.groupIdx = groupIdx
	db.dbPath = dbPath

	options := &pebble.Options{}
	options.Comparer = db.paxosCmp

	var err error
	db.pebbleDB, err = pebble.Open(dbPath, options)
	if err != nil {
		log.Error("open pebbleDB fail.", log.String("db_path", dbPath))
		return err
	}

	db.valueStore = NewLogStore()
	err = db.valueStore.Init(dbPath, groupIdx, db)
	if err != nil {
		log.Error("value store init fail.", log.Err(err))
		return err
	}

	db.inited = true
	log.Info("pebbleKV init success.", log.String("db_path", dbPath))
	return nil
}

func (db *Database) GetDBPath() string {
	return db.dbPath
}

func (db *Database) GetMaxInstanceId() (uint64, error) {
	instanceId := minChosenKey

	it := db.pebbleDB.NewIter(&pebble.IterOptions{})
	it.Last()

	for it.Valid() {
		instanceId = db.getInstanceIdFromKey(it.Key())
		if instanceId == minChosenKey || instanceId == systemVariablesKey || instanceId == masterVariablesKey {
			it.Prev()
		} else {
			it.Close()
			return instanceId, nil
		}
	}
	it.Close()
	return instanceId, ErrNotExist
}

func (db *Database) GetMaxInstanceIdAndFileId() ([]byte, uint64, error) {
	maxInstanceId, err := db.GetMaxInstanceId()
	if err != nil && err != ErrNotExist {
		return nil, 0, err
	}

	if errors.Is(err, ErrNotExist) {
		return nil, maxInstanceId, nil
	}

	key := db.genKey(maxInstanceId)
	fileId, closer, err := db.pebbleDB.Get(key)
	defer closer.Close()
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return fileId, maxInstanceId, ErrNotExist
		}
		log.Error("pebble get fail", log.ByteString("key", key), log.Err(err))
		return nil, 0, err
	}
	return fileId, maxInstanceId, nil
}

func (db *Database) SetSystemVariables(options *WriteOptions, value []byte) error {
	err := db.set(true, systemVariablesKey, value)
	if err != nil {
		log.Error("set system variables fail", log.Err(err))
		return err
	}
	return nil
}

func (db *Database) GetSystemVariables() ([]byte, error) {
	value, err := db.get(systemVariablesKey)
	if err != nil {
		log.Error("get system variables fail", log.Err(err))
		return nil, err
	}
	return value, nil
}

func (db *Database) SetMasterVariables(options *WriteOptions, value []byte) error {
	err := db.set(true, masterVariablesKey, value)
	if err != nil {
		log.Error("set master variables fail", log.Err(err))
		return err
	}
	return nil
}

func (db *Database) GetMasterVariables() ([]byte, error) {
	value, err := db.get(masterVariablesKey)
	if err != nil {
		log.Error("get master variables fail", log.Err(err))
		return nil, err
	}
	return value, nil
}

func (db *Database) get(instanceId uint64) ([]byte, error) {
	key := db.genKey(instanceId)
	value, closer, err := db.pebbleDB.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			log.Error("pebbleKV.Get not found", log.Uint64("instance_id", instanceId))
			return nil, ErrNotExist
		}
		log.Error("pebbleKV.Get fail", log.Uint64("instance_id", instanceId))
		return nil, err
	}
	defer closer.Close()
	return value, nil
}

func (db *Database) set(sync bool, instanceId uint64, value []byte) error {
	key := db.genKey(instanceId)

	pebbleWriteOptions := &pebble.WriteOptions{}
	pebbleWriteOptions.Sync = sync

	err := db.pebbleDB.Set(key, value, pebbleWriteOptions)
	if err != nil {
		log.Error("pebbleDB.Set fail.", log.Uint64("instance_id", instanceId), log.Int("value_len", len(value)))
		return err
	}
	return nil
}

func (db *Database) genKey(instanceId uint64) []byte {
	key := bin.IntToBytes(instanceId)
	return key
}

func (db *Database) getInstanceIdFromKey(key []byte) uint64 {
	instanceId := binary.BigEndian.Uint64(key)
	return instanceId
}

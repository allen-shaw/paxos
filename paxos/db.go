package paxos

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/AllenShaw19/paxos/plugin/log"
	"github.com/cockroachdb/pebble"
	"math"
	"math/rand"
	"os"
	"path/filepath"
)

const (
	kMinChosenKey       uint64 = math.MaxUint64
	kSystemVariablesKey uint64 = math.MaxUint64 - 1
	kMasterVariablesKey uint64 = math.MaxUint64 - 2
)

func newPaxosComparer() *pebble.Comparer {
	c := &pebble.Comparer{
		Name: "PaxosComparer",
		Compare: func(a, b []byte) int {
			if len(a) != sizeOfUint64 {
				log.Error("invalid a.len", log.Int("a.len", len(a)))
				panic("a.len != sizeof uint64")
			}
			if len(b) != sizeOfUint64 {
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
	pebbleKV *pebble.DB
	paxosCmp *pebble.Comparer
	hasInit  bool

	valueStore *LogStore
	dbPath     string

	myGroupIdx int
}

func NewDatabase() *Database {
	return &Database{
		hasInit:    false,
		myGroupIdx: -1,
		paxosCmp:   newPaxosComparer(),
	}
}

func (db *Database) Close() {
	if db.pebbleKV != nil {
		db.pebbleKV.Close()
		db.pebbleKV = nil
	}
	if db.pebbleKV != nil {
		db.pebbleKV.Close()
		db.pebbleKV = nil
	}
	log.Info("pebbleDB deleted.", log.String("path", db.dbPath))
}

func (db *Database) Init(dbPath string, myGroupIdx int) error {
	if db.hasInit {
		return nil
	}
	db.myGroupIdx = myGroupIdx
	db.dbPath = dbPath

	options := &pebble.Options{}
	options.Comparer = db.paxosCmp

	var err error
	db.pebbleKV, err = pebble.Open(dbPath, options)
	if err != nil {
		log.Error("open pebbleDB fail.", log.String("db_path", dbPath))
		return err
	}

	db.valueStore = NewLogStore()

	err = db.valueStore.Init(dbPath, myGroupIdx, db)
	if err != nil {
		log.Error("value store init fail.", log.Err(err))
		return err
	}

	db.hasInit = true
	log.Info("pebbleKV init success.", log.String("db_path", dbPath))
	return nil
}

func (db *Database) GetDBPath() string {
	return db.dbPath
}

func (db *Database) ClearAllLog() error {
	systemVars, err := db.GetSystemVariables()
	if err != nil && err != ErrNotExist {
		log.Error("get system variables fail", log.Err(err))
		return err
	}

	masterVars, err := db.GetMasterVariables()
	if err != nil && err != ErrNotExist {
		log.Error("get master variables fail", log.Err(err))
		return err
	}

	db.hasInit = false
	db.pebbleKV.Close()
	db.pebbleKV = nil

	db.valueStore.Close()
	db.valueStore = nil

	bakPath := db.dbPath + ".bak"
	err = os.RemoveAll(bakPath)
	if err != nil {
		log.Error("delete bak dir fail", log.String("dir", bakPath))
		return err
	}

	err = os.Rename(db.dbPath, bakPath)
	if err != nil {
		log.Error("mv dbpath to bak dir fail", log.String("dbpath", db.dbPath), log.String("bak_dir", bakPath))
		return err
	}

	err = db.Init(db.dbPath, db.myGroupIdx)
	if err != nil {
		log.Error("init again fail", log.Err(err))
		return err
	}

	options := &WriteOptions{}
	options.Sync = true

	if systemVars != "" {
		err = db.SetSystemVariables(options, systemVars)
		if err != nil {
			log.Error("set system variables fail", log.Err(err))
			return err
		}
	}
	if masterVars != "" {
		err = db.SetMasterVariables(options, masterVars)
		if err != nil {
			log.Error("set master variables fail", log.Err(err))
			return err
		}
	}
	return nil
}

func (db *Database) Get(instanceID uint64) (string, error) {
	if !db.hasInit {
		log.Error("db not init yet")
		return "", ErrDBNotInit
	}

	fileID, err := db.getFromPebbleKV(instanceID)
	if err != nil {
		log.Error("get from pebbleKV fail", log.Err(err))
		return "", err
	}

	fileInstanceID, value, err := db.fileIDToValue(fileID)
	if err != nil {
		log.Error("file_id to value fail", log.Err(err))
		return "", err
	}

	if fileInstanceID != instanceID {
		log.Error("file instance_id not equal to key.instance_id",
			log.Uint64("file_instance_id", fileInstanceID),
			log.Uint64("key_instance_id", instanceID))
		return "", errors.New("invalid file instance id")
	}

	return value, nil
}

func (db *Database) Put(options *WriteOptions, instanceID uint64, value string) error {
	if !db.hasInit {
		log.Error("db not init yet")
		return ErrDBNotInit
	}

	fileID, err := db.valueToFileID(options, instanceID, value)
	if err != nil {
		log.Error("value to fileid fail", log.Err(err))
		return err
	}

	err = db.putToPebbleKV(false, instanceID, fileID)
	if err != nil {
		log.Error("put to pebbleKV fail", log.Err(err))
		return err
	}

	return nil
}

func (db *Database) ForceDel(options *WriteOptions, instanceID uint64) error {
	if !db.hasInit {
		log.Error("db not init yet")
		return ErrDBNotInit
	}

	key := db.genKey(instanceID)
	fileID, closer, err := db.pebbleKV.Get([]byte(key))
	defer closer.Close()
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			log.Warn("pebbleKV.Get not found.", log.Uint64("instance_id", instanceID))
			return nil
		}
		log.Error("pebbleKV.Get fail", log.Uint64("instance_id", instanceID), log.Err(err))
		return err
	}

	err = db.valueStore.ForceDel(string(fileID))
	if err != nil {
		log.Error("value store force del fail", log.Err(err))
		return err
	}

	pebbleOptions := &pebble.WriteOptions{}
	pebbleOptions.Sync = options.Sync

	err = db.pebbleKV.Delete([]byte(key), pebbleOptions)
	if err != nil {
		log.Error("pebbleKV.Delete fail", log.Err(err))
		return err
	}

	return nil
}

func (db *Database) Del(options *WriteOptions, instanceID uint64) error {
	if !db.hasInit {
		log.Error("db not init yet")
		return ErrDBNotInit
	}
	key := db.genKey(instanceID)

	if rand.Intn(100) < 1 {
		fileID, closer, err := db.pebbleKV.Get([]byte(key))
		defer closer.Close()
		if err != nil {
			if errors.Is(err, pebble.ErrNotFound) {
				log.Warn("pebbleKV.Get not found.", log.Uint64("instance_id", instanceID))
				return nil
			}
			log.Error("pebbleKV.Get fail", log.Uint64("instance_id", instanceID), log.Err(err))
			return err
		}

		err = db.valueStore.Del(string(fileID))
		if err != nil {
			return err
		}
	}

	pebbleOptions := &pebble.WriteOptions{}
	pebbleOptions.Sync = options.Sync
	err := db.pebbleKV.Delete([]byte(key), pebbleOptions)
	if err != nil {
		log.Error("pebbleKV.Delete fail", log.Err(err))
		return err
	}

	return nil
}

func (db *Database) GetMaxInstanceIDFileID() (string, uint64, error) {
	maxInstanceID, err := db.GetMaxInstanceID()
	if err != nil && err != ErrNotExist {
		return "", 0, err
	}

	if err == ErrNotExist {
		return "", maxInstanceID, nil
	}

	key := db.genKey(maxInstanceID)
	fileID, closer, err := db.pebbleKV.Get([]byte(key))
	defer closer.Close()
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return string(fileID), maxInstanceID, ErrNotExist
		}
		log.Error("pebble get fail", log.String("key", key), log.Err(err))
		return "", 0, err
	}

	return string(fileID), maxInstanceID, nil
}

func (db *Database) GetMaxInstanceID() (uint64, error) {
	instanceID := kMinChosenKey

	it := db.pebbleKV.NewIter(&pebble.IterOptions{})
	it.Last()

	for it.Valid() {
		instanceID = db.getInstanceIDFromKey(string(it.Key()))
		if instanceID == kMinChosenKey ||
			instanceID == kSystemVariablesKey ||
			instanceID == kMasterVariablesKey {
			it.Prev()
		} else {
			it.Close()
			return instanceID, nil
		}
	}

	it.Close()
	return instanceID, ErrNotExist
}

func (db *Database) SetMinChosenInstanceID(options *WriteOptions, minInstanceID uint64) error {
	if !db.hasInit {
		log.Error("db not init yet")
		return ErrDBNotInit
	}

	value := IntToBytes(minInstanceID)
	err := db.putToPebbleKV(true, kMinChosenKey, string(value))
	if err != nil {
		log.Error("put min chosen key to pebbleKV fail", log.Err(err))
		return err
	}

	log.Info("set min chosen instance_id success", log.Uint64("min_chosen_instance_id", minInstanceID))
	return nil
}

func (db *Database) GetMinChosenInstanceID() (uint64, error) {
	if !db.hasInit {
		log.Error("db not init yet")
		return 0, ErrDBNotInit
	}

	value, err := db.getFromPebbleKV(kMinChosenKey)
	if err != nil && err != ErrNotExist {
		log.Error("get from pebbleKV fail", log.Err(err))
		return 0, err
	}

	if err == ErrNotExist {
		log.Error("no min chosen instance_id")
		return 0, nil
	}

	//if db.valueStore.IsValidFileID(value) {
	//	value, err = db.Get(kMinChosenKey)
	//	if err != nil && err != ErrNotExist {
	//		log.Error("get from log store fail", log.Err(err))
	//		return 0, err
	//	}
	//}

	if len(value) != sizeOfUint64 {
		log.Error("invalid min instance_id size")
		return 0, errors.New("invalid min instance_id size")
	}

	minInstanceID := binary.BigEndian.Uint64([]byte(value))
	log.Info("get min chosen instance_id success", log.Uint64("min_chosen_instance_id", minInstanceID))

	return minInstanceID, nil
}

func (db *Database) SetSystemVariables(options *WriteOptions, buffer string) error {
	err := db.putToPebbleKV(true, kSystemVariablesKey, buffer)
	if err != nil {
		log.Error("set system variables fail", log.Err(err))
		return err
	}
	return nil
}

func (db *Database) GetSystemVariables() (string, error) {
	value, err := db.getFromPebbleKV(kSystemVariablesKey)
	if err != nil {
		log.Error("get system variables fail", log.Err(err))
		return "", err
	}
	return value, nil
}

func (db *Database) SetMasterVariables(options *WriteOptions, value string) error {
	err := db.putToPebbleKV(true, kMasterVariablesKey, value)
	if err != nil {
		log.Error("set master variables fail", log.Err(err))
		return err
	}
	return nil
}

func (db *Database) GetMasterVariables() (string, error) {
	value, err := db.getFromPebbleKV(kMasterVariablesKey)
	if err != nil {
		log.Error("get master variables fail", log.Err(err))
		return "", err
	}
	return value, nil
}

func (db *Database) ReBuildOneIndex(instanceID uint64, fileID string) error {
	key := db.genKey(instanceID)

	options := &pebble.WriteOptions{}
	options.Sync = false

	err := db.pebbleKV.Set([]byte(key), []byte(fileID), options)
	if err != nil {
		log.Error("pebbleKV.Set fail", log.Uint64("instance_id", instanceID), log.Int("value_len", len(fileID)))
		return err
	}

	return nil
}

func (db *Database) valueToFileID(options *WriteOptions, instanceID uint64, value string) (string, error) {
	fileID, err := db.valueStore.Append(options, instanceID, value)
	if err != nil {
		log.Error("value to fileID fail", log.Err(err))
		return "", err
	}
	return fileID, nil
}

func (db *Database) fileIDToValue(fileID string) (uint64, string, error) {
	instanceID, value, err := db.valueStore.Read(fileID)
	if err != nil {
		log.Error("file to value fail", log.Err(err))
		return 0, "", err
	}
	return instanceID, value, nil
}

func (db *Database) getFromPebbleKV(instanceID uint64) (string, error) {
	key := db.genKey(instanceID)
	value, closer, err := db.pebbleKV.Get([]byte(key))
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			log.Error("pebbleKV.Get not found", log.Uint64("instance_id", instanceID))
			return "", ErrNotExist
		}
		log.Error("pebbleKV.Get fail", log.Uint64("instance_id", instanceID))
		return "", err
	}
	defer closer.Close()
	return string(value), nil
}

func (db *Database) putToPebbleKV(sync bool, instanceID uint64, value string) error {
	key := db.genKey(instanceID)

	pebbleWriteOptions := &pebble.WriteOptions{}
	pebbleWriteOptions.Sync = sync

	err := db.pebbleKV.Set([]byte(key), []byte(value), pebbleWriteOptions)
	if err != nil {
		log.Error("pebbleKV.Set fail.", log.Uint64("instance_id", instanceID), log.Int("value_len", len(value)))
		return err
	}

	return nil
}

func (db *Database) genKey(instanceID uint64) string {
	key := IntToBytes(instanceID)
	return string(key)
}

func (db *Database) getInstanceIDFromKey(key string) uint64 {
	instanceID := binary.BigEndian.Uint64([]byte(key))
	return instanceID
}

// MultiDatabase

type MultiDatabase struct {
	dbs []*Database
}

func (mdb *MultiDatabase) Close() {
	for _, db := range mdb.dbs {
		db.Close()
	}
}

func (mdb *MultiDatabase) Init(dbPath string, groupCount int) error {
	if !IsExists(dbPath) {
		log.Error("dbpath not exist or limit to open", log.String("dbpath", dbPath))
		return errors.New("db path not exist")
	}

	if groupCount < 1 || groupCount > 100000 {
		log.Error("invalid group count", log.Int("group_count", groupCount))
		return ErrInvalidParam
	}

	for groupIdx := 0; groupIdx < groupCount; groupIdx++ {
		groupDBPath := filepath.Join(dbPath, fmt.Sprintf("g%d", groupIdx))
		db := NewDatabase()

		mdb.dbs = append(mdb.dbs, db)
		if err := db.Init(groupDBPath, groupIdx); err != nil {
			return err
		}
	}

	log.Info("multi db init success.", log.String("db_path", dbPath), log.Int("group_count", groupCount))
	return nil
}

func (mdb *MultiDatabase) GetLogStorageDirPath(groupIdx int) string {
	if groupIdx >= len(mdb.dbs) {
		return ""
	}
	return mdb.dbs[groupIdx].GetDBPath()
}

func (mdb *MultiDatabase) Get(groupIdx int, instanceID uint64) (string, error) {
	if groupIdx >= len(mdb.dbs) {
		return "", ErrInvalidParam
	}
	return mdb.dbs[groupIdx].Get(instanceID)
}

func (mdb *MultiDatabase) Put(options *WriteOptions, groupIdx int, instanceID uint64, value string) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].Put(options, instanceID, value)
}

func (mdb *MultiDatabase) Del(options *WriteOptions, groupIdx int, instanceID uint64) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].Del(options, instanceID)
}

func (mdb *MultiDatabase) ForceDel(options *WriteOptions, groupIdx int, instanceID uint64) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].ForceDel(options, instanceID)
}

func (mdb *MultiDatabase) GetMaxInstanceID(groupIdx int) (uint64, error) {
	if groupIdx >= len(mdb.dbs) {
		return 0, ErrInvalidParam
	}
	return mdb.dbs[groupIdx].GetMaxInstanceID()
}

func (mdb *MultiDatabase) SetMinChosenInstanceID(options *WriteOptions, groupIdx int, minInstanceID uint64) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].SetMinChosenInstanceID(options, minInstanceID)
}

func (mdb *MultiDatabase) GetMinChosenInstanceID(groupIdx int) (uint64, error) {
	if groupIdx >= len(mdb.dbs) {
		return 0, ErrInvalidParam
	}
	return mdb.dbs[groupIdx].GetMinChosenInstanceID()
}

func (mdb *MultiDatabase) ClearAllLog(groupIdx int) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].ClearAllLog()
}

func (mdb *MultiDatabase) SetSystemVariables(options *WriteOptions, groupIdx int, value string) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].SetSystemVariables(options, value)
}

func (mdb *MultiDatabase) GetSystemVariables(groupIdx int) (string, error) {
	if groupIdx >= len(mdb.dbs) {
		return "", ErrInvalidParam
	}
	return mdb.dbs[groupIdx].GetSystemVariables()
}

func (mdb *MultiDatabase) SetMasterVariables(options *WriteOptions, groupIdx int, value string) error {
	if groupIdx >= len(mdb.dbs) {
		return ErrInvalidParam
	}
	return mdb.dbs[groupIdx].SetMasterVariables(options, value)
}

func (mdb *MultiDatabase) GetMasterVariables(groupIdx int) (string, error) {
	if groupIdx >= len(mdb.dbs) {
		return "", ErrInvalidParam
	}
	return mdb.dbs[groupIdx].GetMasterVariables()
}

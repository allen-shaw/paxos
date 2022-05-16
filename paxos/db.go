package paxos

import (
	"encoding/binary"
	"github.com/AllenShaw19/paxos/log"
	"github.com/cockroachdb/pebble"
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
	db       *pebble.DB
	paxosCmp *pebble.Comparer
	hasInit  bool

	valueStore *LogStore
	dbPath   string

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
	if db. != nil {
		db..Close()
		db. = nil
	}
	if db.db != nil {
		db.db.Close()
		db.db = nil
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
	db.db, err = pebble.Open(dbPath, options)
	if err != nil {
		log.Error("open pebbleDB fail.", log.String("db_path",dbPath))
		return err
	}

	db.valueStore = NewLogStore()

	err = db.valueStore.Init(dbPath, myGroupIdx, db)
	if err != nil {
		log.Error("value store init fail.", log.Err(err))
		return err
	}

	db.hasInit =true
	log.Info("db init success.", log.String("db_path", dbPath))
	return nil
}

func (db *Database) GetMaxInstanceIDFileID() (string, uint64, error) {

}

func (db *Database) ReBuildOneIndex(instanceID uint64, fileID string) error {

}

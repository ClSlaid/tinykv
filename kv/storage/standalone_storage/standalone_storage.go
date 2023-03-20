package standalone_storage

import (
	"errors"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db *badger.DB
}

// reader is an implementation of `StorageReader` for a single-node TinyKV instance.
type StandAloneStorageReader struct {
	readOnlyTxn *badger.Txn
}

// NewStandAloneStorage creates a new StandAloneStorage.
//
// Will return a valid storage or panic
func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1)
	if err := conf.Validate(); err != nil {
		panic(err)
	}
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath

	db, err := badger.Open(opts)
	if err != nil {
		panic(err)
	}
	if db == nil {
		err = errors.New("db is nil")
		panic(err)
	}
	st := new(StandAloneStorage)
	st.db = db
	return st
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(_ *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	readTxn := s.db.NewTransaction(false)
	if readTxn == nil {
		err := errors.New("readTxn is nil")
		return nil, err
	}
	r := newStandAloneStorageReader(readTxn)
	return r, nil
}

func (s *StandAloneStorage) Write(_ *kvrpcpb.Context, batch []storage.Modify) error {
	writeTxn := s.db.NewTransaction(true)

	for _, modify := range batch {
		key, val, cf := modify.Key(), modify.Value(), modify.Cf()
		key = engine_util.KeyWithCF(cf, key)
		if val != nil {
			writeTxn.Set(key, val)
		} else {
			writeTxn.Delete(key)
		}
	}

	return writeTxn.Commit()
}

func newStandAloneStorageReader(readTxn *badger.Txn) storage.StorageReader {
	r := new(StandAloneStorageReader)
	r.readOnlyTxn = readTxn
	return r
}

func (sr *StandAloneStorageReader) GetCF(cf string, bs []byte) ([]byte, error) {
	key := engine_util.KeyWithCF(cf, bs)
	item, err := sr.readOnlyTxn.Get(key)
	if err != nil {
		if err.Error() == "Key not found" {
			err = nil
		}
		return nil, err
	}
	return item.ValueCopy(nil)
}

func (sr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, sr.readOnlyTxn)
}

func (sr *StandAloneStorageReader) Close() {
	err := sr.readOnlyTxn.Commit()
	if err != nil {
		panic(err)
	}
}

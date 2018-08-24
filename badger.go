package kvdb

import (
	"errors"
	"hash"

	"github.com/dgraph-io/badger"
)

// NewBadgerDB opens a new badger db handle from the given directory
func NewBadgerDB(datadir string) (*badger.DB, error) {
	opts := badger.DefaultOptions
	opts.Dir = datadir
	opts.ValueDir = datadir
	opts.SyncWrites = true
	return badger.Open(opts)
}

type badgerDB struct {
	prefix string
	db     *badger.DB
}

func (db *badgerDB) CreateTable(name string, obj Object) (Table, error) {
	table, err := db.GetTable(name, obj)

	//
	// Any table initialization should happen here
	//

	return table, err
}

func (db *badgerDB) GetTableVersion(name string, obj ObjectVersion, hf func() hash.Hash) (TableVersion, error) {
	if obj == nil {
		return nil, errors.New("object cannot be nil")
	}

	table := &badgerTableVersion{
		db:  db.db,
		obj: obj.New(),
		hf:  hf,
	}

	switch name {
	case "", "/":
		table.prefix = db.prefix
	default:
		table.prefix = db.prefix + name + "/"
	}

	return table, nil
}

func (db *badgerDB) GetTable(name string, obj Object) (Table, error) {
	if obj == nil {
		return nil, errors.New("object cannot be nil")
	}

	table := &badgerTable{
		db:  db.db,
		obj: obj.New(),
	}

	switch name {
	case "", "/":
		table.prefix = db.prefix
	default:
		table.prefix = db.prefix + name + "/"
	}

	return table, nil
}

type badgerDatastore struct {
	db *badger.DB
}

// NewBadgerDatastore returns a badger backed datastore
func NewBadgerDatastore(dir string) (Datastore, error) {
	db, err := NewBadgerDB(dir)
	if err == nil {
		return NewBadgerDatastoreFromDB(db), nil
	}
	return nil, err
}

// NewBadgerDatastoreFromDB returns a badger backed datastore
func NewBadgerDatastoreFromDB(db *badger.DB) Datastore {
	return &badgerDatastore{db}
}

func (ds *badgerDatastore) GetDB(name string) DB {
	db := &badgerDB{
		db: ds.db,
	}

	// Ensure the prefix ends with a '/'
	switch name {
	case "", "/":
		db.prefix = "/"

	default:
		db.prefix = "/" + name + "/"

	}

	return db
}

func (ds *badgerDatastore) CreateDB(name string) DB {
	db := ds.GetDB(name)

	//
	// Any db initialization should happen here
	//

	return db
}

func translateError(err error) error {
	if err == badger.ErrKeyNotFound {
		return ErrNotFound
	}
	return err
}

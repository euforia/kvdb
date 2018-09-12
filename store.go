package kvdb

import (
	"errors"
	"fmt"
	"hash"
	"log"
	"net/url"

	"github.com/opencontainers/go-digest"
)

var (
	// ErrExists is returned when an object exists
	ErrExists = errors.New("exists")
	// ErrNotFound is returned when an object is not found
	ErrNotFound = errors.New("not found")
)

// Object implements a storable object in the datastore
type Object interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	New() Object
}

// ObjectVersion implements an interface for object versioning support
type ObjectVersion interface {
	// Previous should return the hash of the previous version of the
	// object. It should return nil if this is the first and only version
	PreviousDigest() digest.Digest
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	New() ObjectVersion
	Hash(h hash.Hash)
}

// Table implements a collection of data structures of a given type
type Table interface {
	Create(id []byte, obj Object) error
	Get(id []byte) (Object, error)
	Update(id []byte, obj Object) error
	Delete(id []byte) error
	Iter(start []byte, callback func(Object) error) error
}

// TableVersion implements a colloection of versioned data structures of
// a given type
type TableVersion interface {
	// Returns the object hash and/or error
	Create(id []byte, obj ObjectVersion) ([]byte, error)
	// Returns the obj and object hash
	Get(id []byte) (ObjectVersion, []byte, error)
	// Returns the object hash and/or error
	Update(id []byte, obj ObjectVersion) ([]byte, error)
	// Deletes the object of the id and returns the object hash and/or error
	Delete(id []byte) ([]byte, error)
	// Iterate over the active Objects
	Iter([]byte, func(ObjectVersion) error) error
	// Iterate over each reference.
	IterRef([]byte, func(h []byte) error) error
}

// DB implements a logical grouping of tables
type DB interface {
	CreateTable(name string, obj Object) (Table, error)
	GetTable(name string, obj Object) (Table, error)
	GetTableVersion(string, ObjectVersion, func() hash.Hash) (TableVersion, error)
}

// Datastore implements a store containing dbs
type Datastore interface {
	CreateDB(name string) DB
	GetDB(name string) DB
}

// Open opens a datastore at the given url.  The url scheme must contain the backend
// identifier as it is used to determine the driver to load e.g. badger:///path/to/db
func Open(dburl string, logger *log.Logger) (Datastore, error) {
	uri, _ := url.Parse(dburl)

	switch uri.Scheme {
	case "badger":
		return NewBadgerDatastore(uri.Path, logger)

	default:
		return nil, fmt.Errorf("backend not supported: '%s'", uri.Scheme)

	}
}

package kvdb

import (
	"errors"
	"fmt"
	"net/url"
)

var (
	// ErrExists is returned when an object exists
	ErrExists = errors.New("object exists")
	// ErrNotFound is returned when an object is not found
	ErrNotFound = errors.New("object not found")
)

// Object implements a storable object in the datastore
type Object interface {
	Marshal() ([]byte, error)
	Unmarshal([]byte) error
	New() Object
}

// Table implements a collection of data structures of a given type
type Table interface {
	Create(id []byte, obj Object) error
	Get(id []byte) (Object, error)
	Update(id []byte, obj Object) error
	Delete(id []byte) error
	Iter(start []byte, callback func(Object) error) error
}

// DB implements a logical grouping of tables
type DB interface {
	CreateTable(name string, obj Object) (Table, error)
	GetTable(name string, obj Object) (Table, error)
}

// Datastore implements a store containing dbs
type Datastore interface {
	CreateDB(name string) DB
	GetDB(name string) DB
}

// Open opens a datastore at the given url.  The url scheme must contain the backend
// identifier as it is used to determine the driver to load e.g. badger:///path/to/db
func Open(dburl string) (Datastore, error) {
	uri, _ := url.Parse(dburl)

	switch uri.Scheme {
	case "badger":
		return NewBadgerDatastore(uri.Path)

	default:
		return nil, fmt.Errorf("backend not supported: '%s'", uri.Scheme)

	}
}

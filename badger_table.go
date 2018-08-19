package kvdb

import (
	"fmt"

	"github.com/dgraph-io/badger"
)

type badgerTable struct {
	prefix string
	db     *badger.DB
	obj    Object
}

func (t *badgerTable) Create(id []byte, obj Object) error {
	key := t.getOpaqueKey(id)
	fmt.Printf("CREATE %q\n", key)
	val, err := obj.Marshal()
	if err != nil {
		return err
	}

	return t.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			return ErrExists
		}

		return txn.Set(key, val)
	})
}

func (t *badgerTable) Get(id []byte) (Object, error) {
	key := t.getOpaqueKey(id)
	fmt.Printf("GET %q\n", key)
	obj := t.obj.New()

	err := t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err := item.Value()
		if err != nil {
			return err
		}

		return obj.Unmarshal(val)
	})
	return obj, err
}

func (t *badgerTable) Update(id []byte, obj Object) error {
	key := t.getOpaqueKey(id)

	val, err := obj.Marshal()
	if err != nil {
		return err
	}

	return t.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return ErrNotFound
		}

		return txn.Set(key, val)
	})
}

func (t *badgerTable) Delete(id []byte) error {
	key := t.getOpaqueKey(id)

	return t.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return ErrNotFound
		}

		return txn.Delete(key)
	})
}

func (t *badgerTable) Iter(start []byte, callback func(Object) error) error {
	prefix := t.getOpaqueKey(start)

	return t.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			val, err := item.Value()
			if err != nil {
				return err
			}

			o := t.obj.New()
			err = o.Unmarshal(val)
			if err != nil {
				return err
			}

			if err = callback(o); err != nil {
				return err
			}
		}

		return nil
	})
}

func (t *badgerTable) getOpaqueKey(k []byte) []byte {
	return append([]byte(t.prefix), k...)
}

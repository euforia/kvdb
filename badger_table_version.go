package kvdb

import (
	"bytes"
	"fmt"
	"hash"

	"github.com/dgraph-io/badger"
)

type badgerTableVersion struct {
	prefix string
	db     *badger.DB
	obj    ObjectVersion

	hf func() hash.Hash
}

// Create creats an object by the id. It returns the object hash id or error
func (t *badgerTableVersion) Create(id []byte, obj ObjectVersion) ([]byte, error) {
	refKey := t.getRefKey(id)

	h := t.hf()
	obj.Hash(h)
	objID := h.Sum(nil)
	objKey := t.getObjKey(id, objID)

	val, err := obj.Marshal()
	if err != nil {
		return objID, err
	}

	err = t.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(refKey)
		if err == nil {
			return ErrExists
		}

		err = txn.Set(objKey, val)
		if err == nil {
			err = txn.Set(refKey, objID)
		}
		return err
	})

	fmt.Printf("CREATE VERSION %q\n", objKey)
	fmt.Printf("CREATE VERSION %q\n", refKey)

	return objID, err
}

// Get returns the current object version of an object along with the hash id
func (t *badgerTableVersion) Get(id []byte) (ObjectVersion, []byte, error) {
	key := t.getRefKey(id)
	fmt.Printf("READ VERSION %q\n", key)
	obj := t.obj.New()

	var objID []byte
	err := t.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return translateError(err)
		}

		val, err := item.Value()
		if err != nil {
			return err
		}
		objID = val

		objKey := t.getObjKey(id, objID)

		oitem, err := txn.Get(objKey)
		if err != nil {
			return translateError(err)
		}

		oval, err := oitem.Value()
		if err != nil {
			return err
		}

		return obj.Unmarshal(oval)
	})
	return obj, objID, err
}

// Update updates the current version of the object
func (t *badgerTableVersion) Update(id []byte, obj ObjectVersion) ([]byte, error) {
	key := t.getRefKey(id)

	val, err := obj.Marshal()
	if err != nil {
		return nil, err
	}

	h := t.hf()
	obj.Hash(h)
	objID := h.Sum(nil)

	err = t.db.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err != nil {
			return translateError(err)
		}

		objKey := t.getObjKey(id, objID)
		err = txn.Set(objKey, val)
		if err == nil {
			err = txn.Set(key, objID)
		}

		return err
	})

	fmt.Printf("UPDATE VERSION %q\n", key)

	return objID, err
}

func (t *badgerTableVersion) Delete(id []byte) ([]byte, error) {
	var (
		key   = t.getRefKey(id)
		objID []byte
	)

	err := t.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return translateError(err)
		}

		objID, err = item.Value()
		if err != nil {
			return err
		}

		return txn.Delete(key)
	})
	fmt.Printf("DELETE VERSION %q\n", key)

	return objID, err
}

func (t *badgerTableVersion) IterRef(start []byte, callback func(h []byte) error) error {
	prefix := t.getRefKey(start)
	fmt.Printf("ITER REF %q\n", prefix)

	return t.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			val, err := item.Value()
			if err != nil {
				return err
			}

			if err = callback(val); err != nil {
				return err
			}
		}

		return nil
	})
}

func (t *badgerTableVersion) Iter(start []byte, callback func(ObjectVersion) error) error {
	prefix := t.getRefKey(start)
	fmt.Printf("ITER VERSION %q\n", prefix)

	return t.db.View(func(txn *badger.Txn) error {
		iter := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iter.Close()

		for iter.Seek(prefix); iter.ValidForPrefix(prefix); iter.Next() {
			item := iter.Item()
			val, err := item.Value()
			if err != nil {
				return err
			}

			fk := item.Key()
			k := bytes.TrimPrefix(fk, prefix)
			objID := t.getObjKey(k, val)
			objItem, err := txn.Get(objID)
			if err != nil {
				return err
			}
			objVal, err := objItem.Value()
			if err != nil {
				return err
			}

			o := t.obj.New()
			err = o.Unmarshal(objVal)
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

func (t *badgerTableVersion) getRefKey(id []byte) []byte {
	return append([]byte(t.prefix+"ref/"), id...)
}

// returns key and obj id
func (t *badgerTableVersion) getObjKey(id, objID []byte) []byte {
	prefix := []byte(t.prefix + "object/")
	prefix = append(prefix, id...)
	prefix = append(prefix, '/')

	return append(prefix, objID...)
}

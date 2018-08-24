package kvdb

import (
	"crypto/sha256"
	"encoding/binary"
	"hash"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/opencontainers/go-digest"

	"github.com/stretchr/testify/assert"
)

type testObjectVersion struct {
	*testObject
}

func (obj *testObjectVersion) PreviousDigest() digest.Digest {
	var d digest.Digest
	return d
}

func (obj *testObjectVersion) Hash(h hash.Hash) {
	h.Write([]byte(obj.ID))
	h.Write([]byte(obj.Name))
	binary.Write(h, binary.BigEndian, obj.CreatedAt)
	binary.Write(h, binary.BigEndian, obj.Nonce)
}

func (obj *testObjectVersion) New() ObjectVersion {
	return &testObjectVersion{testObject: &testObject{}}
}

var testVerObjs = map[string]*testObjectVersion{
	"foo": &testObjectVersion{
		testObject: &testObject{
			ID:        "foo",
			Name:      "Foo",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"bar": &testObjectVersion{
		testObject: &testObject{
			ID:        "bar",
			Name:      "Bar",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"base": &testObjectVersion{
		testObject: &testObject{
			ID:        "base",
			Name:      "Base",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"cow": &testObjectVersion{
		testObject: &testObject{
			ID:        "cow",
			Name:      "Cow",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"dog": &testObjectVersion{
		testObject: &testObject{
			ID:        "dog",
			Name:      "Dog",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"elephant": &testObjectVersion{
		testObject: &testObject{
			ID:        "elephant",
			Name:      "Elephant",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"fox": &testObjectVersion{
		testObject: &testObject{
			ID:        "fox",
			Name:      "Fox",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
	"goat": &testObjectVersion{
		testObject: &testObject{
			ID:        "goat",
			Name:      "Goat",
			CreatedAt: time.Now(),
			Nonce:     rand.Uint64(),
		},
	},
}

func Test_TableVersion(t *testing.T) {
	tdir, _ := ioutil.TempDir("/tmp", "thrap-store-")
	defer os.RemoveAll(tdir)

	ds, err := Open("badger://" + tdir)
	if err != nil {
		t.Fatal(err)
	}

	db := ds.CreateDB("deployment")
	table, _ := db.GetTableVersion("instance", &testObjectVersion{}, sha256.New)

	for _, d := range testVerObjs {
		_, err := table.Create([]byte(d.ID), d)
		assert.Nil(t, err)
	}
	for _, d := range testVerObjs {
		obj, _, err := table.Get([]byte(d.ID))
		assert.Nil(t, err)
		vobj := obj.(*testObjectVersion)
		assert.Equal(t, d.Nonce, vobj.Nonce)
	}

	var c int
	table.Iter(nil, func(obj ObjectVersion) error {
		c++
		return nil
	})

	assert.Equal(t, len(testVerObjs), c)

	uIDS := make(map[string][]byte)
	for k, d := range testVerObjs {
		d.CreatedAt = time.Now()
		objID, err := table.Update([]byte(d.ID), d)
		assert.Nil(t, err)
		assert.NotNil(t, objID)
		uIDS[k] = objID
	}

	for k, d := range testVerObjs {
		_, id, err := table.Get([]byte(d.ID))
		assert.Nil(t, err)
		mval := uIDS[k]
		assert.Equal(t, mval, id)
	}
}

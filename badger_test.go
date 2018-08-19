package kvdb

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testObject struct {
	ID   string
	Name string
}

func (obj *testObject) Marshal() ([]byte, error) {
	return json.Marshal(obj)
}
func (obj *testObject) Unmarshal(b []byte) error {
	return json.Unmarshal(b, obj)
}
func (obj *testObject) New() Object {
	return &testObject{}
}

var testObjects = map[string]*testObject{
	"foo": &testObject{
		ID:   "foo",
		Name: "Foo",
	},
	"bar": &testObject{
		ID:   "bar",
		Name: "Bar",
	},
	"base": &testObject{
		ID:   "base",
		Name: "Base",
	},
	"cow": &testObject{
		ID:   "cow",
		Name: "Cow",
	},
	"dog": &testObject{
		ID:   "dog",
		Name: "Dog",
	},
	"elephant": &testObject{
		ID:   "elephant",
		Name: "Elephant",
	},
	"fox": &testObject{
		ID:   "fox",
		Name: "Fox",
	},
	"goat": &testObject{
		ID:   "goat",
		Name: "Goat",
	},
}

func Test_datastore(t *testing.T) {
	tdir, _ := ioutil.TempDir("/tmp", "thrap-store-")
	defer os.RemoveAll(tdir)

	ds, err := Open("badger://" + tdir)
	if err != nil {
		t.Fatal(err)
	}

	db := ds.CreateDB("stack")

	_, err = db.CreateTable("foo", nil)
	assert.NotNil(t, err)

	table, _ := db.CreateTable("descriptor", &testObject{})
	assert.Equal(t, "/stack/descriptor/", table.(*badgerTable).prefix)

	for _, st := range testObjects {
		err := table.Create([]byte(st.ID), st)
		assert.Nil(t, err)
	}

	err = table.Create([]byte("foo"), testObjects["foo"])
	assert.NotNil(t, err)

	objout, err := table.Get([]byte(testObjects["foo"].ID))
	assert.Nil(t, err)
	out := objout.(*testObject)
	assert.Equal(t, "foo", out.ID)

	// Iterator
	var count int
	table.Iter(nil, func(obj Object) error {
		st := obj.(*testObject)
		_, ok := testObjects[st.ID]
		assert.True(t, ok)
		count++
		return nil
	})
	assert.Equal(t, len(testObjects), count)

	err = table.Iter(nil, func(obj Object) error {
		return io.EOF
	})
	assert.Equal(t, io.EOF, err)

	count = 0
	table.Iter([]byte("f"), func(obj Object) error {
		count++
		return nil
	})
	assert.Equal(t, 2, count)

	// Update
	err = table.Update([]byte("foo"), &testObject{ID: "foo", Name: "new foo"})
	assert.Nil(t, err)

	oout, err := table.Get([]byte("foo"))
	assert.Nil(t, err)
	o := oout.(*testObject)
	assert.Equal(t, "new foo", o.Name)

	err = table.Update([]byte("does-not-exist"), &testObject{ID: "foo", Name: "new foo"})
	assert.NotNil(t, err)

	// Delete
	for _, st := range testObjects {
		err := table.Delete([]byte(st.ID))
		assert.Nil(t, err)
	}

	err = table.Delete([]byte("does-not-exist"))
	assert.NotNil(t, err)
}

package boltds

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	ds "github.com/jbenet/go-datastore"
)

func TestBasicPutGet(t *testing.T) {
	path, err := ioutil.TempDir("/tmp", "boltdbtest")
	db, err := NewBoltDatastore(path, "test")
	if err != nil {
		t.Fatal(err)
	}

	dsk := ds.NewKey("test")
	somedata := []byte("some data in the datastore")

	err = db.Put(dsk, somedata)
	if err != nil {
		t.Fatal(err)
	}

	val, err := db.Get(dsk)
	if err != nil {
		t.Fatal(err)
	}

	b, ok := val.([]byte)
	if !ok {
		t.Fatal("Got back invalid typed data")
	}

	if !bytes.Equal(b, somedata) {
		t.Fatal("wrong data")
	}

	err = db.Close()
	if err != nil {
		t.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	b.StopTimer()
	path, err := ioutil.TempDir("/tmp", "boltdbtest")
	db, err := NewBoltDatastore(path, "test")
	if err != nil {
		b.Fatal(err)
	}

	values := make(map[string][]byte)
	for i := 0; i < b.N; i++ {
		values[fmt.Sprint(i)] = []byte(fmt.Sprintf("value number %d", i))
	}

	b.StartTimer()

	for k, v := range values {

		dsk := ds.NewKey(k)
		err := db.Put(dsk, v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPutMany(b *testing.B) {
	b.StopTimer()
	path, err := ioutil.TempDir("/tmp", "boltdbtest")
	db, err := NewBoltDatastore(path, "test")
	if err != nil {
		b.Fatal(err)
	}

	values := make(map[string][]byte)
	for i := 0; i < b.N; i++ {
		values[fmt.Sprint(i)] = []byte(fmt.Sprintf("value number %d", i))
	}

	b.StartTimer()

	data := make(map[ds.Key]interface{})
	for k, v := range values {
		dsk := ds.NewKey(k)
		data[dsk] = v
	}
	err = db.PutMany(data)
	if err != nil {
		b.Fatal(err)
	}
}

package boltds

import (
	bolt "github.com/boltdb/bolt"
	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
	"github.com/jbenet/goprocess"
)

// BoltDatastore implements ds.Datastore
// TODO: use buckets to represent the heirarchy of the ds.Keys
type BoltDatastore struct {
	db         *bolt.DB
	bucketName []byte
	Path       string
}

func NewBoltDatastore(path, bucket string) (*BoltDatastore, error) {
	db, err := bolt.Open(path+"/bolt.db", 0600, nil)
	if err != nil {
		return nil, err
	}

	// TODO: need to do db.Close() sometime...
	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		return err
	})
	if err != nil {
		return nil, err
	}

	return &BoltDatastore{
		db:         db,
		bucketName: []byte(bucket),
		Path:       path + "/bolt.db",
	}, nil
}

func (bd *BoltDatastore) Close() error {
	return bd.db.Close()
}

func (bd *BoltDatastore) Delete(key ds.Key) error {
	return bd.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bd.bucketName).Delete(key.Bytes())
	})
}

func (bd *BoltDatastore) Get(key ds.Key) (interface{}, error) {
	var out []byte
	err := bd.db.View(func(tx *bolt.Tx) error {
		mmval := tx.Bucket(bd.bucketName).Get(key.Bytes())
		if mmval == nil {
			return ds.ErrNotFound
		}
		out = make([]byte, len(mmval))
		copy(out, mmval)
		return nil
	})
	if err != nil {
		return nil, err
	}

	return out, err
}

func (bd *BoltDatastore) ConsumeValue(key ds.Key, f func([]byte) error) error {
	return bd.db.View(func(tx *bolt.Tx) error {
		mmval := tx.Bucket(bd.bucketName).Get(key.Bytes())
		if mmval == nil {
			return ds.ErrNotFound
		}
		return f(mmval)
	})
}

func (bd *BoltDatastore) Has(key ds.Key) (bool, error) {
	var found bool
	err := bd.db.View(func(tx *bolt.Tx) error {
		val := tx.Bucket(bd.bucketName).Get(key.Bytes())
		found = (val != nil)
		return nil
	})
	return found, err
}

func (bd *BoltDatastore) Put(key ds.Key, val interface{}) error {
	bval, ok := val.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}
	return bd.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bd.bucketName).Put(key.Bytes(), bval)
	})
}

func (bd *BoltDatastore) PutMany(data map[ds.Key]interface{}) error {
	return bd.db.Update(func(tx *bolt.Tx) error {
		buck := tx.Bucket(bd.bucketName)
		for k, v := range data {
			bval, ok := v.([]byte)
			if !ok {
				return ds.ErrInvalidType
			}
			err := buck.Put(k.Bytes(), bval)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (bd *BoltDatastore) Query(q query.Query) (query.Results, error) {
	qrb := query.NewResultBuilder(q)
	qrb.Process.Go(func(worker goprocess.Process) {
		bd.db.View(func(tx *bolt.Tx) error {

			buck := tx.Bucket(bd.bucketName)
			c := buck.Cursor()

			var prefix []byte
			if qrb.Query.Prefix != "" {
				prefix = []byte(qrb.Query.Prefix)
			}

			cur := 0
			sent := 0
			for k, v := c.Seek(prefix); k != nil; k, v = c.Next() {
				if cur < qrb.Query.Offset {
					cur++
					continue
				}
				if qrb.Query.Limit > 0 && sent >= qrb.Query.Limit {
					break
				}
				dk := ds.NewKey(string(k)).String()
				e := query.Entry{Key: dk}

				if !qrb.Query.KeysOnly {
					buf := make([]byte, len(v))
					copy(buf, v)
					e.Value = buf
				}

				select {
				case qrb.Output <- query.Result{Entry: e}: // we sent it out
					sent++
				case <-worker.Closing(): // client told us to end early.
					break
				}
				cur++
			}

			return nil
		})
	})

	// go wait on the worker (without signaling close)
	go qrb.Process.CloseAfterChildren()

	qr := qrb.Results()
	for _, f := range q.Filters {
		qr = query.NaiveFilter(qr, f)
	}
	for _, o := range q.Orders {
		qr = query.NaiveOrder(qr, o)
	}
	return qr, nil
}

func (bd *BoltDatastore) IsThreadSafe() {}

type boltBatch struct {
	tx  *bolt.Tx
	bkt *bolt.Bucket
}

func (bd *BoltDatastore) Batch() (ds.Batch, error) {
	tx, err := bd.db.Begin(true)
	if err != nil {
		return nil, err
	}

	buck := tx.Bucket(bd.bucketName)

	return &boltBatch{tx: tx, bkt: buck}, nil
}

func (bb *boltBatch) Commit() error {
	return bb.tx.Commit()
}

func (bb *boltBatch) Delete(k ds.Key) error {
	return bb.bkt.Delete(k.Bytes())
}

func (bb *boltBatch) Put(k ds.Key, val interface{}) error {
	bval, ok := val.([]byte)
	if !ok {
		return ds.ErrInvalidType
	}

	return bb.bkt.Put(k.Bytes(), bval)
}

var _ ds.Batching = (*BoltDatastore)(nil)

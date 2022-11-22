package ydmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/tikv/client-go/v2/txnkv"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

type ObjectInfo struct {
	Name            string                 `json:"name"`
	Size            int64                  `json:"size"`
	Bucket          string                 `json:"bucket"`
	Etag            string                 `json:"etag"`
	ModTime         time.Time              `json:"modifyTime"`
	ContentType     string                 `json:"contentType"`
	ContentEncoding string                 `json:"contentEncoding"`
	Type            string                 `json:"type"`
	ExtFields       map[string]interface{} `json:"extFields"`
	UploadID        string                 `json:"uploadID"`
	PartSize        int64                  `json:"partSize"`
	PartTotal       int64                  `json:"partTotal"`
	Version         string                 `json:"version"`
}

func (i *ObjectInfo) String() string {
	bs, _ := json.Marshal(i)
	return string(bs)
}

type ObjectMetaManager struct {
	MetaManager
}

func NewObjectMetaManager(addr string) (*ObjectMetaManager, error) {
	client, err := newTikvClient(addr)
	if err != nil {
		return nil, err
	}
	return &ObjectMetaManager{MetaManager{client: client}}, nil
}

func NewObjectMetaManagerByClient(client *txnkv.Client) *ObjectMetaManager {
	return &ObjectMetaManager{MetaManager{client: client}}
}

func (o *ObjectMetaManager) ListObjects(bucket string, prefix string, limit int) (keys []string,
	objs []*ObjectInfo, err error) {
	keyPrefix := fmt.Sprintf("%s#%s#%s", OBJECT_PREFIX, bucket, prefix)
	keys = make([]string, 0)
	objs = make([]*ObjectInfo, 0)

	raw, err := o.list([]byte(keyPrefix), limit)
	if err != nil {
		return nil, nil, err
	}

	for _, kv := range raw {
		oi := &ObjectInfo{}
		if err = json.Unmarshal(kv.V, oi); err != nil {
			return nil, nil, err
		}
		objs = append(objs, oi)
		keys = append(keys, string(kv.K))
	}
	return
}

// ListBucketObjectsByIter returns an iter to list
func (o *ObjectMetaManager) ListBucketObjectsByIter(bucket string) (*ObjectMetaIter, error) {
	return newObjectMetaIter([]byte(GenBucketObjectKey(bucket)), o.client)
}

//pure save
func (o *ObjectMetaManager) save(bucket string, objectName string, value []byte, key string, delKey string) error {
	tx, err := o.client.Begin()
	if err != nil {
		return err
	}

	objectInfo, err := tx.Get(context.TODO(), []byte(key))
	if err != nil {
		if !errors.Is(err, tikverr.ErrNotExist) {
			return err
		}
	} else {
		err = tx.Set([]byte(delKey), objectInfo)
		if err != nil {
			return err
		}
	}

	err = tx.Set([]byte(key), value)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func (o *ObjectMetaManager) SaveObject(bucket string, objectName string, value []byte) error {
	tx, err := o.client.Begin()
	if err != nil {
		return err
	}
	key := GenObjectKey(bucket, objectName)

	objectInfo, err := tx.Get(context.TODO(), []byte(key))
	if err != nil {
		if !errors.Is(err, tikverr.ErrNotExist) {
			return err
		}
	} else {
		delKey := GenDeletedObjectKey(bucket, objectName)
		err = tx.Set([]byte(delKey), objectInfo)
		if err != nil {
			return err
		}
	}

	err = tx.Set([]byte(key), value)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func (o *ObjectMetaManager) GetObject(bucket string, objectName string) (*ObjectInfo, error) {
	key := GenObjectKey(bucket, objectName)

	val, err := o.get([]byte(key))
	if err != nil {
		return nil, err
	}

	objectInfo := new(ObjectInfo)
	err = json.Unmarshal(val, objectInfo)
	if err != nil {
		return nil, fmt.Errorf("parse object info error: %s", err.Error())
	}

	return objectInfo, nil
}

func (o *ObjectMetaManager) DeleteObject(bucket string, objectName string) error {
	key := GenObjectKey(bucket, objectName)
	return o.dels([]byte(key))
}

//Mark object as deleted
func (o *ObjectMetaManager) MarkObjectDeleted(bucket string, objectName string) error { //TODO
	tx, err := o.client.Begin()
	if err != nil {
		return err
	}
	//get original object
	oriKey := GenObjectKey(bucket, objectName)
	val, err := tx.Get(context.TODO(), []byte(oriKey))
	if err != nil {
		return err
	}
	// set deleted object
	delKey := GenDeletedObjectKey(bucket, objectName)
	err = tx.Set([]byte(delKey), val)
	if err != nil {
		return err
	}
	//delete original object
	err = tx.Delete([]byte(oriKey))
	if err != nil {
		return err
	}
	return tx.Commit(context.Background())
}

//Mark object as deleted by specific object and value.
func (o *ObjectMetaManager) MarkObjectDeletedWithValue(bucket string, objectName string, value []byte) error { //TODO
	tx, err := o.client.Begin()
	if err != nil {
		return err
	}

	// set deleted object
	delKey := GenDeletedObjectKey(bucket, objectName)
	err = tx.Set([]byte(delKey), value)
	if err != nil {
		return err
	}

	return tx.Commit(context.Background())
}

func (o *ObjectMetaManager) GetObjectName(bucket string, objectName string) string {
	var fname string
	prefix := fmt.Sprintf("%s#%s#", OBJECT_PREFIX, bucket)
	if len(prefix) > len(objectName) {
		return ""
	}
	fname = objectName[len(prefix):] //objectName:includes filepath/filename
	return fname
}

func (o *ObjectMetaManager) ListDeletedObjects(start string, limit int) (deletedKeys []string,
	deletedObjectInfo []*ObjectInfo, err error) {
	lhs := GenDeletedObjectPrefix(start)
	raw, err := o.list([]byte(lhs), limit)
	if err != nil {
		return nil, nil, err
	}
	for _, kv := range raw {
		oi := &ObjectInfo{}
		if err = json.Unmarshal(kv.V, oi); err != nil {
			return nil, nil, err
		}
		deletedObjectInfo = append(deletedObjectInfo, oi)
		deletedKeys = append(deletedKeys, string(kv.K))
	}
	return
}

// ListDeletedObjectsByIter need to ensure that each fetched key/value is deleted after use
func (o *ObjectMetaManager) ListDeletedObjectsByIter() (*ObjectMetaIter, error) {
	return newObjectMetaIter([]byte(GetDeletedObjectKey()), o.client)
}

// DeleteByDeletedKey delete object == pure deletion
func (o *ObjectMetaManager) DeleteByDeletedKey(key string) error {
	return o.dels([]byte(key))
}

type ObjectMetaIter struct {
	interClose func()
	interValid func() bool
	interNext  func() error
	interKey   func() []byte
	interValue func() []byte
}

func newObjectMetaIter(key []byte, tc *txnkv.Client) (*ObjectMetaIter, error) {
	tx, err := tc.Begin()
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(key, upper(key))
	if err != nil {
		return nil, err
	}
	return &ObjectMetaIter{
		interClose: it.Close,
		interValid: it.Valid,
		interNext:  it.Next,
		interKey:   it.Key,
		interValue: it.Value,
	}, nil
}

func (i *ObjectMetaIter) Next() error {
	return i.interNext()
}

func (i *ObjectMetaIter) Value() *ObjectInfo {
	oi := &ObjectInfo{}
	_ = json.Unmarshal(i.interValue(), oi)
	return oi
}

func (i *ObjectMetaIter) Key() string {
	return string(i.interKey())
}

func (i *ObjectMetaIter) Valid() bool {
	return i.interValid()
}

func (i *ObjectMetaIter) Close() {
	i.interClose()
}

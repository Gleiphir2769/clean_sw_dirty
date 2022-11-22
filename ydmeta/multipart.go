package ydmeta

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/tikv/client-go/v2/txnkv"
	"strings"
	"time"
)

type MultipartMetaV1 struct {
	Version         string                 `json:"version"` // Version number
	Bucket          string                 `json:"bucket"`  // Bucket name
	Object          string                 `json:"object"`  // Object name
	ContentType     string                 `json:"contentType"`
	ContentEncoding string                 `json:"contentEncoding"`
	ModTime         time.Time              `json:"modifyTime"`
	ExtFields       map[string]interface{} `json:"extFields"`
}
type FileIdInfo struct {
	FileId   string
	Offset   int64
	FileSize int64
}

type MultipartPartMetaV1 struct {
	Size     int64
	Etag     string
	FidInfos []FileIdInfo
}

func (o *ObjectMetaManager) SaveMultipart(bucket string, objectName string, value []byte) error {
	key := GenMultipartKey(bucket, objectName)
	delKey := genDeletedMultipartKey(bucket, objectName)
	return o.save(bucket, objectName, value, key, delKey)
}

func (o *ObjectMetaManager) ListMultiparts(bucket string, prefix string, startNumber int, limit int) ([]KV, error) {
	nextKeyPrefix := fmt.Sprintf("%s#%s#%s#~", MULTIPART_PREFIX, bucket, prefix)
	keyPrefix := fmt.Sprintf("%s#%s#%s#%05d", MULTIPART_PREFIX, bucket, prefix, startNumber+1)
	var ret []KV
	//iter
	tx, err := o.client.Begin()
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter([]byte(keyPrefix), []byte(nextKeyPrefix))
	if err != nil {
		return nil, err
	}
	defer it.Close()
	if limit == 0 {
		limit = 10000
	}
	for it.Valid() && limit > 0 {
		strKey := string(it.Key())
		if strings.HasPrefix(strKey, fmt.Sprintf("%s#%s#%s", MULTIPART_PREFIX, bucket, prefix)) {
			ret = append(ret, KV{K: it.Key()[:], V: it.Value()[:]})
			limit--
		}
		it.Next()
	}

	return ret, nil
}

func (o *ObjectMetaManager) GetMultipartMeta(bucket string, objectName string) (*MultipartMetaV1, error) {
	key := GenMultipartKey(bucket, objectName)
	val, err := o.get([]byte(key))
	if err != nil {
		return nil, err
	}

	swfsMultipartInfo := new(MultipartMetaV1)
	err = json.Unmarshal(val, swfsMultipartInfo)
	if err != nil {
		return nil, fmt.Errorf("parse MultipartMeta info error: %s", err.Error())
	}

	return swfsMultipartInfo, nil

}
func (o *ObjectMetaManager) GetMultipartPartMeta(bucket string, objectName string) (*MultipartPartMetaV1, error) {
	key := GenMultipartKey(bucket, objectName)
	val, err := o.get([]byte(key))
	if err != nil {
		return nil, err
	}

	partMetaInfo := new(MultipartPartMetaV1)
	err = json.Unmarshal(val, partMetaInfo)
	if err != nil {
		return nil, fmt.Errorf("parse object info error: %s", err.Error())
	}

	return partMetaInfo, nil
}

//Mark object as deleted
func (o *ObjectMetaManager) MarkMultipartDeleted(bucket string, objectName string) error {
	tx, err := o.client.Begin()
	if err != nil {
		return err
	}
	//get original object
	oriKey := GenMultipartKey(bucket, objectName)
	val, err := tx.Get(context.TODO(), []byte(oriKey))
	if err != nil {
		return err
	}
	// set deleted object
	delKey := genDeletedMultipartKey(bucket, objectName)
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

//Delete multipart key
func (o *ObjectMetaManager) DeleteMultipartMeta(bucket string, objectName string) error {
	tx, err := o.client.Begin()
	if err != nil {
		return err
	}
	//get original object
	oriKey := GenMultipartKey(bucket, objectName)
	err = tx.Delete([]byte(oriKey))
	if err != nil {
		return err
	}
	return tx.Commit(context.Background())
}

func (o *ObjectMetaManager) ListMultipartByIter() (*MultipartMetaIter, error) {
	return newMultipartMetaIter([]byte(MULTIPART_PREFIX), o.client)
}

type MultipartMetaIter struct {
	interClose func()
	interValid func() bool
	interNext  func() error
	interKey   func() []byte
	interValue func() []byte
}

func newMultipartMetaIter(key []byte, tc *txnkv.Client) (*MultipartMetaIter, error) {
	tx, err := tc.Begin()
	if err != nil {
		return nil, err
	}
	it, err := tx.Iter(key, upper(key))
	if err != nil {
		return nil, err
	}
	return &MultipartMetaIter{
		interClose: it.Close,
		interValid: it.Valid,
		interNext:  it.Next,
		interKey:   it.Key,
		interValue: it.Value,
	}, nil
}

func (i *MultipartMetaIter) Next() error {
	return i.interNext()
}

func (i *MultipartMetaIter) Value() *MultipartPartMetaV1 {
	oi := &MultipartPartMetaV1{}
	err := json.Unmarshal(i.interValue(), oi)
	if err != nil {
		return nil
	}
	return oi
}

func (i *MultipartMetaIter) Key() string {
	return string(i.interKey())
}

func (i *MultipartMetaIter) Valid() bool {
	return i.interValid()
}

func (i *MultipartMetaIter) Close() {
	i.interClose()
}

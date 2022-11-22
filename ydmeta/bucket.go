package ydmeta

import (
	"encoding/json"
	"errors"
	"github.com/tikv/client-go/v2/txnkv"
	"time"

	tikverr "github.com/tikv/client-go/v2/error"
)

type BucketInfo struct {
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	CreateTime time.Time              `json:"createTime"`
	ExtFields  map[string]interface{} `json:"extFields"`
}

func (b *BucketInfo) Encode() ([]byte, error) {
	return json.Marshal(b)
}

type BucketMetaManager struct {
	MetaManager
}

func NewBucketMetaManager(addr string) (*BucketMetaManager, error) {
	client, err := newTikvClient(addr)
	if err != nil {
		return nil, err
	}
	return &BucketMetaManager{MetaManager{client: client}}, nil
}

func NewBucketMetaManagerByClient(client *txnkv.Client) *BucketMetaManager {
	return &BucketMetaManager{MetaManager{client: client}}
}

func (bm *BucketMetaManager) CreateBucket(bucket string, info *BucketInfo) error {
	val, err := info.Encode()
	if err != nil {
		return err
	}

	bucketKey := GenBucketKey(bucket)

	return bm.setIfAbsent([]byte(bucketKey), val)
}

func (bm *BucketMetaManager) GetBucketInfo(bucket string) (bucketInfo *BucketInfo, err error) {
	bucketKey := GenBucketKey(bucket)

	val, err := bm.get([]byte(bucketKey))
	if err != nil {
		if !errors.Is(err, tikverr.ErrNotExist) {
			return nil, err
		}
		return nil, nil
	}

	bucketInfo = &BucketInfo{}
	if err = json.Unmarshal(val, bucketInfo); err != nil {
		return nil, err
	}
	return bucketInfo, nil
}

func (bm *BucketMetaManager) DeleteBucket(bucket string) error {
	bucketKey := GenBucketKey(bucket)
	return bm.dels([]byte(bucketKey))
}

func (bm *BucketMetaManager) ListBuckets() (buckets []*BucketInfo, err error) {
	kvs, err := bm.list([]byte(BUCKET_PREFIX), -1)
	if err != nil {
		return nil, err
	}

	buckets = make([]*BucketInfo, 0, len(kvs))
	for i := 0; i < len(kvs); i++ {
		bucketInfo := &BucketInfo{}
		if err = json.Unmarshal(kvs[i].V, bucketInfo); err != nil {
			return nil, err
		}
		buckets = append(buckets, bucketInfo)
	}
	return buckets, nil
}

func (bm *BucketMetaManager) ListBucketsByType(t string) (buckets []*BucketInfo, err error) {
	kvs, err := bm.list([]byte(BUCKET_PREFIX), -1)
	if err != nil {
		return nil, err
	}

	buckets = make([]*BucketInfo, 0, len(kvs))
	for i := 0; i < len(kvs); i++ {
		bucketInfo := &BucketInfo{}
		if err = json.Unmarshal(kvs[i].V, bucketInfo); err != nil {
			return nil, err
		}
		if bucketInfo.Type == t {
			buckets = append(buckets, bucketInfo)
		}
	}
	return buckets, nil
}

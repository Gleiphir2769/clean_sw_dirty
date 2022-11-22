package ydmeta

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateDeleteBucket(t *testing.T) {
	metaAddr := os.Getenv("META_SERVER_ADDRESS")
	if len(metaAddr) == 0 {
		panic("META_SERVER_ADDRESS is required")
	}
	bm, err := NewBucketMetaManager(metaAddr)
	require.Nil(t, err)

	bucketKey := []byte("YDS3_BUCKET#abc")
	bm.dels([]byte(bucketKey))
	bucketName := "abc"

	bucketInfo := &BucketInfo{}
	bucketInfo.Name = bucketName
	bucketInfo.Type = "seaweedfs"
	bucketInfo.CreateTime = time.Now()
	err = bm.CreateBucket(bucketName, bucketInfo)
	require.Nil(t, err)

	bucketInfo2, err := bm.GetBucketInfo(bucketName)
	require.Nil(t, err)
	require.NotNil(t, bucketInfo2)
	t.Logf("%v\n", bucketInfo2)

	err = bm.CreateBucket(bucketName, bucketInfo)
	require.NotNil(t, err)

	_, err = bm.get(bucketKey)
	require.Nil(t, err)

	err = bm.DeleteBucket(bucketName)
	require.Nil(t, err)

	bm.Close()
}

func TestListBuckets(t *testing.T) {
	metaAddr := os.Getenv("META_SERVER_ADDRESS")
	if len(metaAddr) == 0 {
		panic("META_SERVER_ADDRESS is required")
	}
	bm, err := NewBucketMetaManager(metaAddr)
	require.Nil(t, err)

	for i := 0; i < 8; i++ {
		bucketName := fmt.Sprintf("mybucket-%d", i)
		bucketInfo := &BucketInfo{}
		bucketInfo.Name = bucketName
		bucketInfo.Type = "seaweedfs"
		bucketInfo.CreateTime = time.Now()
		err = bm.CreateBucket(bucketName, bucketInfo)
		require.Nil(t, err)
	}

	buckets, err := bm.ListBuckets()
	require.Nil(t, err)
	require.NotNil(t, buckets)
	assert.Equal(t, 8, len(buckets))
	for _, bucketInfo := range buckets {
		t.Log(bucketInfo)
	}

	for i := 0; i < 8; i++ {
		bucketName := fmt.Sprintf("mybucket-%d", i)
		err = bm.DeleteBucket(bucketName)
		require.Nil(t, err)
	}

	bm.Close()
}

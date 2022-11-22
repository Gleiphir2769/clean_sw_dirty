package ydmeta

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/tikv/client-go/v2/txnkv"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var (
	testBucketName                    = "abc"
	bm             *BucketMetaManager = nil
	om             *ObjectMetaManager = nil
)

func TestObjectInfo(t *testing.T) {
	var dataSize int64 = 0

	// encode
	fidInfos := make([]FileIdInfo, 0)
	for i := 0; i < 8; i++ {
		fidInfo := FileIdInfo{}
		fidInfo.FileId = fmt.Sprintf("%d,20bf9be799", i)
		fidInfo.Offset = dataSize
		fidInfo.FileSize = 512 * 1024
		dataSize += fidInfo.FileSize
		fidInfos = append(fidInfos, fidInfo)
	}

	objectInfo := &ObjectInfo{}
	objectInfo.ExtFields = make(map[string]interface{})
	objectInfo.ExtFields["fids"] = fidInfos

	objectInfo.Size = dataSize
	objectInfo.Etag = "asdfasdfads"
	objectInfo.ModTime = time.Now()
	objectInfo.Bucket = "testbucket"

	val, err := json.Marshal(objectInfo)
	require.Nil(t, err)
	t.Logf("%s", string(val))

	// decode
	objectInfo2 := &ObjectInfo{}
	err = json.Unmarshal(val, &objectInfo2)
	require.Nil(t, err)

	fidInfos2 := make([]FileIdInfo, 0)
	fidsBytes, err := json.Marshal(objectInfo2.ExtFields["fids"])
	require.Nil(t, err)
	err = json.Unmarshal(fidsBytes, &fidInfos2)
	require.Nil(t, err)
	require.Equal(t, 8, len(fidInfos2))
}

func initTestBucket() {
	bucketInfo := &BucketInfo{}
	bucketInfo.Name = testBucketName
	bucketInfo.Type = "seaweedfs"
	bucketInfo.CreateTime = time.Now()
	bm.CreateBucket(testBucketName, bucketInfo)
}

func clearupTestBucket() {
	bm.DeleteBucket(testBucketName)
}

func TestMain(m *testing.M) {
	metaAddr := os.Getenv("META_SERVER_ADDRESS")
	if len(metaAddr) == 0 {
		metaAddr = "localhost:2379"
	}
	var err error
	bm, err = NewBucketMetaManager(metaAddr)
	if err != nil {
		panic(err)
	}
	om, err = NewObjectMetaManager(metaAddr)
	if err != nil {
		panic(err)
	}

	initTestBucket()

	m.Run()

	clearupTestBucket()

	om.Close()
	bm.Close()
}

func buildTestObjectInfoWithName(name string) ([]byte, error) {
	objectInfo := &ObjectInfo{}
	objectInfo.Name = name
	objectInfo.Size = 320 * 1024
	objectInfo.Etag = "asdfasdfads"
	objectInfo.ModTime = time.Now()
	objectInfo.Bucket = testBucketName
	return json.Marshal(objectInfo)
}

func clearupObjects(t *testing.T) {
	kvs, err := om.scan([]byte("YDS3_OBJECT#"), []byte("YDS3_OBJECT~"), 1024)
	require.Nil(t, err)
	for _, kv := range kvs {
		om.dels(kv.K)
	}

	kvs, err = om.scan([]byte("YDS3_DELETED_OBJECT#"), []byte("YDS3_DELETED_OBJECT~"), 1024)
	require.Nil(t, err)
	for _, kv := range kvs {
		om.dels(kv.K)
	}
}

func TestCreateDeleteObject(t *testing.T) {
	clearupObjects(t)

	objectInfoBytes, err := buildTestObjectInfoWithName("objtest")
	require.Nil(t, err)
	err = om.SaveObject(testBucketName, "objtest", objectInfoBytes)
	require.Nil(t, err)

	objInfo, err := om.GetObject(testBucketName, "objtest")
	require.Nil(t, err)
	require.NotNil(t, objInfo)

	objInfoBytes, err := om.get([]byte("YDS3_OBJECT#abc#objtest"))
	require.Nil(t, err)
	t.Logf("%s", string(objInfoBytes))

	kvs, err := om.scan([]byte("YDS3_DELETED_OBJECT#"), []byte("YDS3_DELETED_OBJECT~"), 10)
	require.Nil(t, err)
	require.Equal(t, 0, len(kvs))

	objectInfoBytes2, err := buildTestObjectInfoWithName("objtest")
	require.Nil(t, err)
	err = om.SaveObject(testBucketName, "objtest", objectInfoBytes2)
	require.Nil(t, err)

	kvs, err = om.scan([]byte("YDS3_DELETED_OBJECT#"), []byte("YDS3_DELETED_OBJECT~"), 10)
	require.Nil(t, err)
	require.Equal(t, 1, len(kvs))

	err = om.MarkObjectDeleted(testBucketName, "objtest")
	require.Nil(t, err)

	kvs, err = om.scan([]byte("YDS3_DELETED_OBJECT#"), []byte("YDS3_DELETED_OBJECT~"), 10)
	require.Nil(t, err)
	require.Equal(t, 2, len(kvs))
}

func TestListKey(t *testing.T) {
	tc, err := txnkv.NewClient([]string{"localhost:2379"})
	assert.NoError(t, err)
	om := NewObjectMetaManagerByClient(tc)
	kvs, err := om.list([]byte(fmt.Sprintf("%s#%s#", MULTIPART_PREFIX, "shenjiaqi123")), 10)
	assert.NoError(t, err)
	for _, kv := range kvs {
		fmt.Println("key", string(kv.K))
	}
}

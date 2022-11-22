package ydmeta

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

const (
	KEY_SEPARATOR = "#"

	BUCKET_PREFIX         = "YDS3_BUCKET"
	OBJECT_PREFIX         = "YDS3_OBJECT"
	DELETED_OBJECT_PREFIX = "YDS3_DELETED_OBJECT"
	DELETED_BUCKET_PREFIX = "YDS3_DELETED_BUCKET"

	MULTIPART_PREFIX         = "YDS3_MULTIPART"
	DELETED_MULTIPART_PREFIX = "YDS3_DELETED_MULTIPART"

	ObjectLargeType = "large"
)

// GenObjectKey generate object key
func GenObjectKey(bucket string, object string) string {
	return fmt.Sprintf("%s#%s#%s", OBJECT_PREFIX, bucket, object)
}

// GenBucketObjectKey generate objects key of specific bucket
func GenBucketObjectKey(bucket string) string {
	return fmt.Sprintf("%s#%s", OBJECT_PREFIX, bucket)
}

// GenDeletedObjectKey generate deleted object key
func GenDeletedObjectKey(bucket string, object string) string {
	tsp := time.Now().UnixNano()
	return fmt.Sprintf("%s#%d#%s#%s", DELETED_OBJECT_PREFIX, tsp, bucket, object)
}

func GenBucketKey(bucket string) string {
	return fmt.Sprintf("%s#%s", BUCKET_PREFIX, bucket)
}

func GetDeletedObjectKey() string {
	return fmt.Sprintf("%s#", DELETED_OBJECT_PREFIX)
}

func GenDeletedObjectPrefix(prefix string) string {
	return fmt.Sprintf("%s#%s", DELETED_OBJECT_PREFIX, prefix)
}

func ParseObjectKey(key string) (string, bool) {
	seg := strings.Split(key, "#")
	if len(seg) < 3 {
		return "", false
	}
	return seg[2], true
}

func ParseDeletedObjectKey(key string) (int64, bool) {
	seg := strings.Split(key, "#")
	if len(seg) < 2 {
		return 0, false
	}
	tsp, err := strconv.ParseInt(seg[1], 10, 64)
	if err != nil {
		return 0, false
	}
	return tsp, true
}

//GenMultipartKey generate multipart key
func GenMultipartKey(bucket string, object string) string {
	return fmt.Sprintf("%s#%s#%s", MULTIPART_PREFIX, bucket, object)
}

//generate deleted multipart key
func genDeletedMultipartKey(bucket string, object string) string {
	tsp := time.Now().UnixNano()
	return fmt.Sprintf("%s#%d#%s#%s", MULTIPART_PREFIX, tsp, bucket, object)
}

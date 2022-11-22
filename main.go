package main

import (
	"clean_sw_dirty/ydmeta"
	"fmt"
	"os"
)

func main() {
	pd := os.Getenv("CLEANER_PD")
	//master := os.Getenv("CLEANER_MASTER")

	bm, err := ydmeta.NewBucketMetaManager(pd)
	if err != nil {
		panic(err)
	}
	om, err := ydmeta.NewObjectMetaManager(pd)
	if err != nil {
		panic(err)
	}
	//sc, err := swfsclient.NewSwfsClient(master,
	//	&http.Client{Timeout: 5 * time.Minute}, 1024)
	//if err != nil {
	//	panic(err)
	//}

	buckets, err := bm.ListBuckets()
	if err != nil {
		panic(err)
	}

	validMultipart := make(map[string]struct{})

	for _, b := range buckets {
		iter, err := om.ListBucketObjectsByIter(b.Name)
		if err != nil {
			panic(err)
		}
		for iter.Valid() {
			ob := iter.Value()

			if ob.Type == ydmeta.ObjectLargeType {
				for i := 0; i < int(ob.PartTotal); i++ {
					multiKey := ydmeta.GenMultipartKey(ob.Bucket, swfsMultipartDataName(ob.UploadID, i))
					validMultipart[multiKey] = struct{}{}
				}
			}

			iter.Next()
		}
		iter.Close()

		delIter, err := om.ListDeletedObjectsByIter()
		if err != nil {
			panic(err)
		}
		for delIter.Valid() {
			ob := delIter.Value()

			if ob.Type == ydmeta.ObjectLargeType {
				for i := 0; i < int(ob.PartTotal); i++ {
					multiKey := ydmeta.GenMultipartKey(ob.Bucket, swfsMultipartDataName(ob.UploadID, i))
					validMultipart[multiKey] = struct{}{}
				}
			}

			delIter.Next()
		}
		delIter.Close()
	}

	var totalSize int64
	needDelCounts := 0
	mpIter, err := om.ListMultipartByIter()
	for mpIter.Valid() {
		mp := mpIter.Value()
		if mp == nil {
			continue
		}

		if _, ok := validMultipart[mpIter.Key()]; ok {
			continue
		}

		totalSize += mp.Size
		needDelCounts++
	}

	fmt.Println(fmt.Sprintf("clean finished, multiparts count is %d, multiparts size is %.2fGB",
		needDelCounts, float64(totalSize)/1024/1024))
}

func swfsMultipartDataName(uploadID string, partNumber int) string {
	return fmt.Sprintf("%s#%05d", uploadID, partNumber)
}

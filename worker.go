package main

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func runWorkers(ctx context.Context, client *s3.Client, bucket string, workChan <-chan WorkItem, filesChan chan<- taggedFile, n int, prog *Progress) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, client, bucket, workChan, filesChan, prog)
		}()
	}
	wg.Wait()
	close(filesChan)
}

func worker(ctx context.Context, client *s3.Client, bucket string, workChan <-chan WorkItem, filesChan chan<- taggedFile, prog *Progress) {
	for item := range workChan {
		listRecursive(ctx, client, bucket, item.Prefix, filesChan, prog)
	}
}

func listRecursive(ctx context.Context, client *s3.Client, bucket, prefix string, filesChan chan<- taggedFile, prog *Progress) {
	var token *string
	for {
		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			break
		}
		prog.listRequests.Add(1)
		for _, obj := range resp.Contents {
			sc := string(obj.StorageClass)
			if sc == "" {
				sc = "STANDARD"
			}
			var size int64
			if obj.Size != nil {
				size = *obj.Size
			}
			prog.bytesAccounted.Add(size)
			prog.objectsAccounted.Add(1)
			if obj.Key != nil {
				parent, name := splitKey(*obj.Key)
				filesChan <- taggedFile{ParentPrefix: parent, FileEntry: FileEntry{Name: name, SizeBytes: size, StorageClass: sc}}
			}
		}
		if resp.IsTruncated == nil || !*resp.IsTruncated || resp.NextContinuationToken == nil {
			break
		}
		token = resp.NextContinuationToken
	}
}

package main

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func runWorkers(ctx context.Context, client *s3.Client, bucket string, workChan <-chan WorkItem, statsChan chan<- StatBatch, n int, prog *Progress) {
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, client, bucket, workChan, statsChan, prog)
		}()
	}
	wg.Wait()
	close(statsChan)
}

func worker(ctx context.Context, client *s3.Client, bucket string, workChan <-chan WorkItem, statsChan chan<- StatBatch, prog *Progress) {
	for item := range workChan {
		listRecursive(ctx, client, bucket, item.Prefix, statsChan, prog)
	}
}

func listRecursive(ctx context.Context, client *s3.Client, bucket, prefix string, statsChan chan<- StatBatch, prog *Progress) {
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

		batchMap := map[string]*StatBatch{}
		for _, obj := range resp.Contents {
			sc := string(obj.StorageClass)
			if sc == "" {
				sc = "STANDARD"
			}
			b := batchMap[sc]
			if b == nil {
				batchMap[sc] = &StatBatch{Prefix: prefix, StorageClass: sc}
				b = batchMap[sc]
			}
			b.Count++
			if obj.Size != nil {
				b.SizeBytes += *obj.Size
				prog.bytesAccounted.Add(*obj.Size)
			}
			prog.objectsAccounted.Add(1)
		}
		for _, b := range batchMap {
			statsChan <- *b
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated || resp.NextContinuationToken == nil {
			break
		}
		token = resp.NextContinuationToken
	}
}

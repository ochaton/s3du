package main

import (
	"context"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func resolveBucketRegion(ctx context.Context, client *s3.Client, bucket string) (string, error) {
	resp, err := client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return "", err
	}
	region := string(resp.LocationConstraint)
	if region == "" {
		region = "us-east-1"
	}
	return region, nil
}

func discover(ctx context.Context, client *s3.Client, bucket string, statsChan chan<- StatBatch, prog *Progress) <-chan WorkItem {
	workChan := make(chan WorkItem, 256)

	go func() {
		defer close(workChan)

		topPrefixes := listWithDelimiter(ctx, client, bucket, "", statsChan, prog)

		var wg sync.WaitGroup
		sem := make(chan struct{}, 32)
		for _, prefix := range topPrefixes {
			wg.Add(1)
			sem <- struct{}{}
			go func(p string) {
				defer wg.Done()
				defer func() { <-sem }()
				subPrefixes := listWithDelimiter(ctx, client, bucket, p, statsChan, prog)
				for _, sub := range subPrefixes {
					workChan <- WorkItem{Prefix: sub, Depth: 2}
				}
			}(prefix)
		}
		wg.Wait()
	}()

	return workChan
}

// listWithDelimiter lists one level with delimiter="/", emits objects as StatBatches, returns CommonPrefixes.
func listWithDelimiter(ctx context.Context, client *s3.Client, bucket, prefix string, statsChan chan<- StatBatch, prog *Progress) []string {
	var prefixes []string
	var token *string

	for {
		resp, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			Prefix:            aws.String(prefix),
			Delimiter:         aws.String("/"),
			ContinuationToken: token,
		})
		if err != nil {
			break
		}
		prog.listRequests.Add(1)

		// group objects by storage class into batches
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

		for _, cp := range resp.CommonPrefixes {
			if cp.Prefix != nil {
				prefixes = append(prefixes, *cp.Prefix)
			}
		}

		if resp.IsTruncated == nil || !*resp.IsTruncated || resp.NextContinuationToken == nil {
			break
		}
		token = resp.NextContinuationToken
	}
	return prefixes
}

package main

import (
	"context"
	"strings"
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

func discover(ctx context.Context, client *s3.Client, bucket string, filesChan chan<- taggedFile, prog *Progress) <-chan WorkItem {
	workChan := make(chan WorkItem, 256)

	go func() {
		defer close(workChan)

		topPrefixes := listWithDelimiter(ctx, client, bucket, "", filesChan, prog)

		var wg sync.WaitGroup
		sem := make(chan struct{}, 32)
		for _, prefix := range topPrefixes {
			wg.Add(1)
			sem <- struct{}{}
			go func(p string) {
				defer wg.Done()
				defer func() { <-sem }()
				subPrefixes := listWithDelimiter(ctx, client, bucket, p, filesChan, prog)
				for _, sub := range subPrefixes {
					workChan <- WorkItem{Prefix: sub, Depth: 2}
				}
			}(prefix)
		}
		wg.Wait()
	}()

	return workChan
}

func listWithDelimiter(ctx context.Context, client *s3.Client, bucket, prefix string, filesChan chan<- taggedFile, prog *Progress) []string {
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

		for _, obj := range resp.Contents {
			if obj.Key == nil {
				continue
			}
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
			parent, name := splitKey(*obj.Key)
			filesChan <- taggedFile{ParentPrefix: parent, FileEntry: FileEntry{Name: name, SizeBytes: size, StorageClass: sc}}
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

// splitKey splits "a/b/c/file.txt" into ("a/b/c/", "file.txt").
// Root-level keys like "readme.txt" → ("", "readme.txt").
func splitKey(key string) (parent, name string) {
	idx := strings.LastIndex(key, "/")
	if idx < 0 {
		return "", key
	}
	return key[:idx+1], key[idx+1:]
}

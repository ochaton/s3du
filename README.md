# s3du

`du` for S3. Scans bucket in parallel, groups by prefix and storage class.

![s3du TUI](screenshot.png)

## Install

```bash
go install github.com/ochaton/s3du@latest
```

Or build from source:

```bash
go build -o s3du .
```

## Run

```bash
# Plain output
s3du -bucket my-bucket

# Interactive TUI
s3du -bucket my-bucket -i

# Force re-scan (ignore cache)
s3du -bucket my-bucket -i --refresh

# Tune parallelism / region
s3du -bucket my-bucket -workers 64 -region us-west-2
```

AWS credentials from env/profile chain (`AWS_PROFILE`, `~/.aws/credentials`, IAM role, etc).

## Cache

After each scan, results saved to:

```
~/.cache/s3du/<bucket>@<region>/stats.json.gz   # aggregated stats
~/.cache/s3du/<bucket>@<region>/objects.bin      # binary object index
```

Next `-i` run loads cache — no re-scan. Use `--refresh` to bust it.

# s3du

`du` for S3. Scans bucket in parallel, groups by prefix and storage class,
keeps everything in a compressed radix-tree snapshot you can re-open
without re-paying the list cost.

![s3du TUI](screenshot.png)

## Install

```bash
# Main binary
go install github.com/ochaton/s3du@latest

# Side binaries
go install github.com/ochaton/s3du/cmd/s3du-rm@latest    # parallel batched delete by prefix
go install github.com/ochaton/s3du/cmd/s3du-sim@latest   # offline strategy A/B against a snapshot
go install github.com/ochaton/s3du/cmd/radix-bench@latest # microbench for the radix tree
```

Or build from source:

```bash
git clone https://github.com/ochaton/s3du && cd s3du
go build -o s3du .
```

## Run

```bash
# Scan a bucket and drop into the TUI
s3du -bucket my-bucket -i

# Re-open the snapshot without re-scanning (default path resolved automatically)
s3du -bucket my-bucket -load -i

# Snapshot + print stats only
s3du -snapshot path -load -stats

# Tune parallelism, override region
s3du -bucket my-bucket -workers 64 -region us-west-2
```

AWS credentials come from the env/profile chain (`AWS_PROFILE`,
`~/.aws/credentials`, IAM role, etc). The bucket's region is auto-detected
via HeadBucket when `-region` is left blank.

## Snapshot

Each scan writes a binary snapshot under the platform's user-cache dir
(`os.UserCacheDir`):

```
Linux : ~/.cache/s3du/<bucket>@<region>/tree.snap
macOS : ~/Library/Caches/s3du/<bucket>@<region>/tree.snap
```

Re-open with `-load -snapshot <path>` — no re-scan, full TUI / -stats / -i
flows work against the loaded snapshot.

## TUI keybindings

```
Navigation       Sort                 Display
↑/k  ↓/j         s  by size           g  cycle bar widget
Home  End/G      n  by name           ?  show this help
PgUp PgDn        C  by object count
Enter / l        $  by monthly cost
Bksp / h         t  toggle dirs first
                                       q  quit
```

`g` cycles: no bar → bar only (default) → bar + percentage → percentage
only. Bar is normalised to the listing total (entries sum to 100%).

## Logs

Structured slog logs are off by default — pass `-log <path>` to enable
file logging. SDK retry events (SlowDown, adaptive throttling) surface
through the file logger so you can diagnose a scan that goes quiet.

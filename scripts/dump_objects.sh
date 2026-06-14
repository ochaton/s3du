#!/usr/bin/env bash
set -euo pipefail

: "${PROFILE:?set PROFILE=<aws-profile>}"
: "${BUCKET:?set BUCKET=<bucket-name>}"
MAX="${MAX:-50000}"
OUT="${OUT:-test.objects.jsonl}"

aws --profile "$PROFILE" s3api list-objects-v2 \
    --bucket "$BUCKET" \
    --max-items "$MAX" \
    --output json \
    --query 'Contents[].{key:Key,size:Size,class:StorageClass}' \
  | jq -c '.[]' > "$OUT"

wc -l "$OUT"
du -h "$OUT"

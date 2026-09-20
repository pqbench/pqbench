#!/bin/sh
set -eu

# Keep every engine in one bucket but give each its own prefix. This mirrors the
# usual remote layout while making cleanup and inspection simple.
awslocal s3api head-bucket --bucket lakehouse 2>/dev/null ||
    awslocal s3 mb s3://lakehouse

#!/bin/bash
set -e

gcloud storage rsync parquet gs://${BUCKET_NAME}/datasets/tenhou --recursive -x '.*\.DS_Store'

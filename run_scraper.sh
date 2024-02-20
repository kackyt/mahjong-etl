#!/bin/bash

set -e

mkdir logs
gcloud storage cp gs://${BUCKET_NAME}/logs/${DATE}* ./logs --recursive -x '.*\.DS_Store'

poetry run python main.py -L logs -O parquet

gcloud storage rsync parquet gs://${BUCKET_NAME}/datasets/tenhou --recursive -x '.*\.DS_Store'

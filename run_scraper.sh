#!/bin/bash

set -e

if [ ! -d logs ]; then
mkdir logs
fi

gcloud storage cp gs://${BUCKET_NAME}/logs/${DATE}* ./logs --recursive

poetry run python main.py -L logs -O parquet

gcloud storage rsync parquet gs://${BUCKET_NAME}/datasets/tenhou --recursive -x '.*\.DS_Store'

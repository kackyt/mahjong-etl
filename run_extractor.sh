#!/bin/bash

python extractor.py -O logs --old
python extractor.py -O logs

gcloud storage rsync logs gs://${BUCKET_NAME}/logs  --recursive -x '.*\.DS_Store'

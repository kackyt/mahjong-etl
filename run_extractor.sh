#!/bin/bash
set -e

python extractor.py -O logs --old --date ${DATE}
python extractor.py -O logs --date ${DATE}

gcloud storage rsync logs gs://${BUCKET_NAME}/logs  --recursive -x '.*\.DS_Store'

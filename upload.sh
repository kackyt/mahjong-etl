#!/bin/bash
gcloud storage rsync parquet gs://${BUCKET_NAME}/datasets/tenhou --recursive
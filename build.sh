#!/bin/bash
set -e

apt-get update && apt-get upgrade -y && \
    apt-get install -y --no-install-recommends curl \
    libffi-dev libssl-dev \
    python3-crcmod \
    apt-transport-https \
    lsb-release \
    openssh-client \
    gnupg

# Cloud SDKインストール
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y --no-install-recommends

apt-get clean

# poetry install
curl -sSL https://install.python-poetry.org | python -

poetry config virtualenvs.create false
poetry install --only main --no-root

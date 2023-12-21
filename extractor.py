from urllib.parse import urlparse
from typing import List, Dict, Tuple, Any
from datetime import datetime
import xml.etree.ElementTree as ET
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import requests
import gzip
import re
import os

from scrape import parse_document


headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"
}

DOWNLOAD_PREFIX = "https://tenhou.net/sc/raw/dat/"


def extract_latest_logs(enable_transform: bool):
    r = requests.get("https://tenhou.net/sc/raw/list.cgi", headers=headers)
    atag = re.compile(r"<a\s+href=[\"'](?P<href>.*?)[\"']")

    text = r.text.replace("list([\r\n", "").replace(");", "")
    files = text.split(",\r\n")

    for archive_item in files:
        if "scc" in archive_item:
            archive_name = archive_item.split("',")[0].replace("{file:'", "")

            file_name = archive_name

            print(f"Downloading {file_name}")
            url = os.path.join(DOWNLOAD_PREFIX, file_name)

            # ファイル名から日付(yyyymmdd)を取り出す
            dtstr = re.search(r"\d{8}", file_name)

            if dtstr is None:
                raise Exception("date cannot found")
            dt = datetime.strptime(dtstr[0], r"%Y%m%d")

            print(dt)

            page = requests.get(url, headers=headers)

            data = gzip.decompress(page.content).decode("utf-8")

            lines = data.split("\r\n")

            for line in lines:
                m = atag.search(line)
                if m is not None:
                    href = m.group("href")
                    u = urlparse(href)

                    log_id = u.query.split("=")[1]
                    url = f"https://tenhou.net/0/log/?{log_id}"
                    print(f"\tDownload {url}")

                    log_file = requests.get(url, headers=headers)
                    os.makedirs(f"logs/{dtstr[0]}", exist_ok=True)

                    with open(f"logs/{dtstr[0]}/{log_id}.xml", "w") as f:
                        f.write(log_file.text)

                    if enable_transform:
                        doc = ET.fromstring(log_file.text)
                        parse_document(doc, log_id, dt)

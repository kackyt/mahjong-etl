from urllib.parse import urlparse
from tqdm import tqdm
from typing import Optional
from datetime import datetime, timezone
import xml.etree.ElementTree as ET
import requests
import gzip
import re
import os

from scrape import parse_document, save_to_parquet


headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}

DOWNLOAD_PREFIX = "https://tenhou.net/sc/raw/dat/"


def extract_latest_logs(log_dir: str, output_dir: Optional[str]):
    r = requests.get("https://tenhou.net/sc/raw/list.cgi", headers=headers)
    atag = re.compile(r"<a\s+href=[\"'](?P<href>.*?)[\"']")

    text = r.text.replace("list([\r\n", "").replace(");", "")
    files = text.split(",\r\n")
    prev_dt = None
    seqno = 0

    for archive_item in files:
        if "html" in archive_item:
            archive_name = archive_item.split("',")[0].replace("{file:'", "")

            file_name = archive_name

            print(f"Downloading {file_name}")
            url = os.path.join(DOWNLOAD_PREFIX, file_name)

            # ファイル名から日付(yyyymmdd)を取り出す
            dtstr = re.search(r"\d{8}", file_name)

            if dtstr is None:
                raise Exception("date cannot found")
            dt = datetime.strptime(dtstr[0], r"%Y%m%d").replace(tzinfo=timezone.utc)

            if output_dir is not None and prev_dt is not None and prev_dt != dt:
                save_to_parquet(output_dir, prev_dt)
                seqno = 0
            prev_dt = dt

            page = requests.get(url, headers=headers)

            data = gzip.decompress(page.content).decode("utf-8")

            lines = data.split("\r\n")

            for line in tqdm(lines):
                m = atag.search(line)
                if m is not None:
                    href = m.group("href")
                    u = urlparse(href)

                    log_id = u.query.split("=")[1]
                    url = f"https://tenhou.net/0/log/?{log_id}"
                    os.makedirs(os.path.join(log_dir, dtstr[0]), exist_ok=True)

                    # ファイルの存在チェック
                    filepath = os.path.join(log_dir, dtstr[0], f"{log_id}.xml")

                    if os.path.exists(filepath):
                        # 存在する場合は読み出す
                        if output_dir is not None:
                            tree = ET.parse(filepath)
                            seqno = parse_document(tree.getroot(), log_id, dt, seqno)
                    else:
                        log_file = requests.get(url, headers=headers)

                        with open(filepath, "w") as f:
                            f.write(log_file.text)

                        if output_dir is not None:
                            doc = ET.fromstring(log_file.text)
                            seqno = parse_document(doc, log_id, dt, seqno)

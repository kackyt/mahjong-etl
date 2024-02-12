from urllib.parse import urlparse
from tqdm import tqdm
import requests
import gzip
import re
import os
import argparse

headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"}

DOWNLOAD_PREFIX = "https://tenhou.net/sc/raw/dat/"


def extract_logs(is_old: bool, log_dir: str):
    old_query = "?old" if is_old else ""
    r = requests.get(f"https://tenhou.net/sc/raw/list.cgi{old_query}", headers=headers)
    atag = re.compile(r"<a\s+href=[\"'](?P<href>.*?)[\"']")

    r.raise_for_status()

    text = r.text.replace("list([\r\n", "").replace(");", "")
    files = text.split(",\r\n")

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

            page = requests.get(url, headers=headers)

            page.raise_for_status()

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

                    log_file = requests.get(url, headers=headers)

                    with open(filepath, "w") as f:
                        f.write(log_file.text)


parser = argparse.ArgumentParser(description="tenho mahjong log etl tool")

parser.add_argument("--old", help="log file download from old or latest archive", action="store_true")
parser.add_argument("--output-dir", "-O", help="transform log output directory", type=str)

args = parser.parse_args()

extract_logs(args.old, args.output_dir)

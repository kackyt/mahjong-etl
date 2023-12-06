from urllib.parse import urlparse
import requests
import gzip
import re
import os

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:47.0) Gecko/20100101 Firefox/47.0"
}
r = requests.get("https://tenhou.net/sc/raw/list.cgi", headers=headers)

atag = re.compile(r"<a\s+href=[\"'](?P<href>.*?)[\"']")

DOWNLOAD_PREFIX = "https://tenhou.net/sc/raw/dat/"

text = r.text.replace("list([\r\n", "").replace(");", "")
files = text.split(",\r\n")

for archive_item in files:
    if "scc" in archive_item:
        archive_name = archive_item.split("',")[0].replace("{file:'", "")

        file_name = archive_name

        print(f"Downloading {file_name}")
        url = os.path.join(DOWNLOAD_PREFIX, file_name)

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

                with open(f"logs/{log_id}.xml", "w") as f:
                    f.write(log_file.text)
        break

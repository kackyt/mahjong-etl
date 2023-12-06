from urllib.parse import urlparse
from typing import List, Dict
import xml.etree.ElementTree as ET
import pandas as pd
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

                doc = ET.fromstring(log_file.text)

                with open(f"logs/{log_id}.xml", "w") as f:
                    f.write(log_file.text)
        break

# parquetに書き出すテーブルおよびレコード
class Serializable:
    def to_parquet(self, outpath: str):
        df = pd.DataFrame(self.__dict__)
        df.to_parquet(outpath)

class Player(Serializable):
    name: str

class Game(Serializable):
    id: str
    initial_score: int
    started_at: str

class GamePlayer(Serializable):
    game_id: str
    player_name: str
    player_index: int

class Kyoku(Serializable):
    id: int
    game_id: str
    index: int
    honba: int
    riichibou: int
    kazes: List[int]

class Haipai(Serializable):
    kyoku_id: int
    player_index: int
    haipai: str

class Agari(Serializable):
    kyoku_id: int
    machi_no: str
    score: int
    fu: int
    han: int
    tehai: str
    dora: List[int]
    uradora: List[int]
    who: int
    by: int
    score_diff: List[int]
    owari: bool


players: Dict[str, Player] = {}


def parse_document(root: ET.Element):
    for child in root:
        if child.tag == "GO":
        elif child.tag == "UN":
        elif child.tag == "TAIKYOKU":
            # do nothing
        elif child.tag == "INIT":
        elif child.tag == "DORA":
        elif child.tag == "N":
            


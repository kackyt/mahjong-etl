from urllib.parse import urlparse
from typing import List, Dict
import xml.etree.ElementTree as ET
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import gzip
import re
import os
import time

# parquetに書き出すテーブルおよびレコード
class Serializable:
    def to_parquet(self, outpath: str):
        df = pd.DataFrame(self.__dict__)
        df.to_parquet(outpath)


Player = pa.schema([
    pa.field("name", pa.string())
])

Game = pa.schema([
    pa.field("id", pa.string()),
    pa.field("initial_score", pa.int32()),
    pa.field("started_at", pa.date64())
])

GamePlayer = pa.schema([
    pa.field("game_id", pa.string()),
    pa.field("player_name", pa.string()),
    pa.field("player_index", pa.int32())
])

Kyoku = pa.schema([
    pa.field("id", pa.int64()),
    pa.field("game_id", pa.string()),
    pa.field("kyoku_num", pa.int32()),
    pa.field("honba", pa.int32()),
    pa.field("riichibou", pa.int32()),
    pa.field("kazes", pa.list_(pa.int32()))
])

Haipai = pa.schema([
    pa.field("kyoku_id", pa.int64()),
    pa.field("player_index", pa.int32()),
    pa.field("haipai", pa.string()),
])

Agari = pa.schema([
    pa.field("kyoku_id", pa.int64()),
    pa.field("machihai", pa.string()),
    pa.field("score", pa.int32()),
    pa.field("fu", pa.int32()),
    pa.field("han", pa.int32()),
    pa.field("dora", pa.list_(pa.string())),
    pa.field("uradora", pa.list_(pa.string())),
    pa.field("who", pa.int32()),
    pa.field("by", pa.int32()),
    pa.field("score_diff", pa.list_(pa.int32())),
    pa.field("owari", pa.bool_())
])

Action = pa.schema([
    pa.field("kyoku_id", pa.int64()),
    pa.field("player_index", pa.int32()),
    pa.field("seq", pa.int32()),
    pa.field("type", pa.string()),
    pa.field("pais", pa.string())
])

players: Dict[str, pa.RecordBatch] = {}
games: List[pa.RecordBatch] = []
game_players: List[pa.RecordBatch] = []
kyokus: List[pa.RecordBatch] = []
haipais: List[pa.RecordBatch] = []
actions: List[pa.RecordBatch] = []


yaku_table = [
    '門前清自摸和', '立直', '一発', '槍槓', '嶺上開花',
    '海底摸月', '河底撈魚', '平和', '断幺九', '一盃口',
    '自風 東', '自風 南', '自風 西', '自風 北', '場風 東',
    '場風 南', '場風 西', '場風 北', '役牌 白', '役牌 發',
    '役牌 中', '両立直', '七対子', '混全帯幺九', '一気通貫',
    '三色同順', '三色同刻', '三槓子', '対々和', '三暗刻',
    '小三元', '混老頭', '二盃口', '純全帯幺九', '混一色',
    '清一色', '', '天和', '地和', '大三元',
    '四暗刻', '四暗刻単騎', '字一色', '緑一色', '清老頭',
    '九蓮宝燈', '純正九蓮宝燈', '国士無双', '国士無双１３面', '大四喜',
    '小四喜', '四槓子', 'ドラ', '裏ドラ', '赤ドラ',
]


def num_to_hai(num_list: List[int], has_aka: bool) -> str:
    colors = ["m", "p", "s", "z"]
    suit = None

    pais: List[str] = []

    for pn in sorted(num_list):
        paistr = ""
        s = colors[pn // 36]
        if s != suit:
            paistr = s
            suit = s
        
        n = (pn % 36) // 4 + 1
        if has_aka and s != "z" and n == 5 and (pn % 4) == 0:
            n = 0
        
        paistr = paistr + str(n)

        pais.append(paistr)

    return "".join(pais)


def parse_document(root: ET.Element, game_id: str):
    has_aka = False
    current_kyoku: pa.RecordBatch
    kyoku_id: int = int(time.time() * 1000)
    doras: List[str] = []
    action_count: int = 0
    reach = False
    kan = False
    oya: int = 0
    tsumohai: int = 0
    for child in root:
        if child.tag == "GO":
            tp = int(child.attrib["type"])

            if (tp & 0x02) != 0:
                has_aka = True
            else:
                has_aka = False
        elif child.tag == "UN":
            game_player = pa.RecordBatch.from_arrays([
                pa.array([game_id, game_id, game_id, game_id]),
                pa.array([child.attrib["n0"], child.attrib["n1"], child.attrib["n2"], child.attrib["n3"]]),
                pa.array([0, 1, 2, 3])
            ], schema=GamePlayer)
            game_players.append(game_player)
        elif child.tag == "TAIKYOKU":
            # do nothing
            _ = 0
        elif child.tag == "INIT":
            # 局の開始
            seeds = child.attrib["seed"].split(",")
            kyoku_num = int(seeds[0])
            honba = int(seeds[1])
            reachbou = int(seeds[2])
            dora = int(seeds[5])
            kaze_table = [[0, 1, 2, 3], [3, 0, 1, 2], [2, 3, 0, 1], [1, 2, 3, 0]]
            oya = int(child.attrib["oya"])

        elif child.tag == "DORA":
            _ = 0
        elif child.tag == "REACH":
            reach = child.attrib["step"] == "1"
        elif child.tag == "AGARI":
            ten = child.attrib["ten"].split(",")
            sc = child.attrib["sc"].split(",")
            yaku = child.attrib["yaku"].split(",")

        elif child.tag == "N":
            # なき
            who = int(child.attrib["who"])
            m = int(child.attrib["m"])
            types = ["", "+", "=", "-"]
            colors = ["m", "p", "s", "z"]
            d = types[m & 0x03]

            if (m & 0x0004) != 0:
                # チー
                pt = (m & 0xFC00) >> 10
                r = pt % 3
                pn = pt // 3
                s = colors[pn // 7]
                n = pn % 7 + 1
                pai_ids = [m & 0x0018, m & 0x0060, m & 0x0180]

                pais: List[str] = []
                for i in range(3):
                    x = str(pai_ids[i])
                    if has_aka and pai_ids[i] == 0 and n + i == 5:
                        x = "0"
                    if i == r:
                        x = x + d
                    pais.append(x)

                paist = s + "".join(pais)

                action = pa.RecordBatch.from_arrays([
                    pa.array([kyoku_id]),
                    pa.array([who]),
                    pa.array([action_count]),
                    pa.array(["tii"]),
                    pa.array([paist])
                ], schema=Action)

                actions.append(action)
                action_count += 1
            elif (m & 0x0018) != 0:
                # ポン or 加えカン
                pt = (m & 0xFE00) >> 9
                r = pt % 3
                pn = pt // 3
                s = colors[pn // 9]
                n = pn % 9 + 1
                nn = [n, n, n, n]

                if has_aka and s != "z" and n == 5:
                    if (m & 0x0060) == 0:
                        nn[3] = 0
                    elif r == 0:
                        nn[2] = 0
                    else:
                        nn[1] = 0

                if (m & 0x0008) != 0:
                    paist = s + "".join(map(lambda x: str(x), nn[0:3])) + d
                    action = pa.RecordBatch.from_arrays([
                        pa.array([kyoku_id]),
                        pa.array([who]),
                        pa.array([action_count]),
                        pa.array(["pon"]),
                        pa.array([paist])
                    ], schema=Action)
                    actions.append(action)
                else:
                    paist = s + "".join(map(lambda x: str(x), nn[0:3])) + \
                        d + str(nn[3])
                    action = pa.RecordBatch.from_arrays([
                        pa.array([kyoku_id]),
                        pa.array([who]),
                        pa.array([action_count]),
                        pa.array(["kan"]),
                        pa.array([paist])
                    ], schema=Action)
                    actions.append(action)
                    kan = True
                action_count += 1
            else:
                # 暗カンまたは大明槓
                pt = m >> 8
                r = pt % 4
                pn = pt // 4
                s = colors[pn // 9]
                n = pn % 9 + 1
                nn = [n, n, n, n]
                if has_aka and s != "z" and n == 5:
                    if (d == ""):
                        nn[3] = 0
                    elif r == 0:
                        nn[3] = 0
                    else:
                        nn[2] = 0

                paist = s + "".join(map(lambda x: str(x), nn)) + d
                action = pa.RecordBatch.from_arrays([
                    pa.array([kyoku_id]),
                    pa.array([who]),
                    pa.array([action_count]),
                    pa.array(["kan"]),
                    pa.array([paist])
                ], schema=Action)
                kan = True

                actions.append(action)
                action_count += 1
        elif re.match(r"^[TUVW]\d+$", child.tag):
            # ツモ
            who = (ord(child.tag[0]) - ord("T") + 4 - oya) % 4
            tsumohai = int(child.tag[1:])
            p = num_to_hai([tsumohai], has_aka)

            typ = "tsumo_k" if kan else "tsumo"

            action = pa.RecordBatch.from_arrays([
                pa.array([kyoku_id]),
                pa.array([who]),
                pa.array([action_count]),
                pa.array([typ]),
                pa.array([p])
            ], schema=Action)
            actions.append(action)
            action_count += 1
            kan = False
        elif re.match(r"^[DEFG]\d+$", child.tag):
            # 捨て牌
            who = (ord(child.tag[0]) - ord("D") + 4 - oya) % 4
            sutehai = int(child.tag[1:])
            p = num_to_hai([tsumohai], has_aka)

            if sutehai == tsumohai:
                p += "_"
            if reach:
                p += "*"
            reach = False
            action = pa.RecordBatch.from_arrays([
                pa.array([kyoku_id]),
                pa.array([who]),
                pa.array([action_count]),
                pa.array(["sutehai"]),
                pa.array([p])
            ], schema=Action)
            actions.append(action)
            action_count += 1


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

                parse_document(doc, log_id)
                wp = pq.ParquetWriter("players.parquet", schema=Player)
                for record in players:
                    wp.write_batch(record)
                wp.close()
                wg = pq.ParquetWriter("games.parquet", schema=Game)
                for record in games:
                    wg.write_batch(record)
                wg.close()
                wgp = pq.ParquetWriter("game_players.parquet", schema=GamePlayer)
                for record in game_players:
                    wgp.write_batch(record)
                wgp.close()
                wk = pq.ParquetWriter("kyokus.parquet", schema=Kyoku)
                for record in kyokus:
                    wk.write_batch(record)
                wk.close()
                wh = pq.ParquetWriter("haipais.parquet", schema=Haipai)
                for record in haipais:
                    wh.write_batch(record)
                wh.close()
                wa = pq.ParquetWriter("actions.parquet", schema=Action)
                for record in actions:
                    wa.write_batch(record)
                wa.close()


        break

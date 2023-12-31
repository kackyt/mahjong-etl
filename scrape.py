from typing import List, Dict, Tuple, Any, Set
from datetime import datetime
import xml.etree.ElementTree as ET
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import re
import os
import time
import urllib.parse

# parquetに書き出すテーブルおよびレコード
Player = pa.schema([
    pa.field("name", pa.string())
])

Game = pa.schema([
    pa.field("id", pa.string()),
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
    pa.field("reachbou", pa.int32()),
    pa.field("scores", pa.list_(pa.int32(), 4)),
    pa.field("kazes", pa.list_(pa.int32(), 4))
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
    pa.field("tehai", pa.string()),
    pa.field("yaku", pa.list_(pa.string())),
    pa.field("dora", pa.list_(pa.string())),
    pa.field("uradora", pa.list_(pa.string())),
    pa.field("who", pa.int32()),
    pa.field("by", pa.int32()),
    pa.field("score_diff", pa.list_(pa.int32(), 4)),
    pa.field("owari", pa.bool_())
])

Action = pa.schema([
    pa.field("kyoku_id", pa.int64()),
    pa.field("player_index", pa.int32()),
    pa.field("seq", pa.int32()),
    pa.field("type", pa.string()),
    pa.field("pais", pa.string())
])

Nagare = pa.schema([
    pa.field("kyoku_id", pa.int64()),
    pa.field("name", pa.string()),
    pa.field("score_diff", pa.list_(pa.int32(), 4))
])

players: Set[str] = set()
games: List[Dict[str, Any]] = []
game_players: List[Dict[str, Any]] = []
kyokus: List[Dict[str, Any]] = []
haipais: List[Dict[str, Any]] = []
actions: List[Dict[str, Any]] = []
agaris: List[Dict[str, Any]] = []
nagares: List[Dict[str, Any]] = []

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

nagare_table = {
    "nm":     '流し満貫',
    "yao9":   '九種九牌',
    "kaze4":  '四風連打',
    "reach4": '四家立直',
    "ron3":   '三家和了',
    "kan4":   '四槓散了',
}


def dora_hai(num_list: List[int]) -> List[str]:
    # 表示牌の次
    def nhai(num: int) -> int:
        s = num // 36
        n = (num % 36) // 4

        if s == 3:
            n += 1
            if n == 4:
                n = 0
            elif n == 7:
                n = 4
        else:
            n += 1
            if n >= 9:
                n = 0
        return s * 36 + n * 4

    return [num_to_hai([nhai(x)], False) for x in num_list]


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


def nakimentsu(m: int, has_aka: bool) -> Tuple[str, str]:
    types = ["", "+", "=", "-"]
    colors = ["m", "p", "s", "z"]
    d = types[m & 0x03]
    paist: str
    atype: str

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
        atype = "tii"
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
            atype = "pon"
        else:
            paist = s + "".join(map(lambda x: str(x), nn[0:3])) + \
                d + str(nn[3])
            atype = "kan"
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
        atype = "kan"
    return paist, atype


def parse_document(root: ET.Element, game_id: str, dt: datetime):
    has_aka = False
    # current_kyoku: pa.RecordBatch
    kyoku_id: int = int(time.time() * 1000)
    doras: List[int] = []
    action_count: int = 0
    reach = False
    kan = False
    oya: int = 0
    tsumohai: int = 0
    dt64 = pa.scalar(dt, type=pa.date64())
    player_name: Dict[str, str] = {}

    games.append({
        "id": game_id,
        "started_at": dt64
    })
    for child in root:
        if child.tag == "GO":
            tp = int(child.attrib["type"])

            if (tp & 0x02) != 0:
                has_aka = True
            else:
                has_aka = False
        elif child.tag == "UN":
            n0 = child.attrib.get("n0")
            n1 = child.attrib.get("n1")
            n2 = child.attrib.get("n2")
            n3 = child.attrib.get("n3")
            if n0 is not None:
                player_name["n0"] = urllib.parse.unquote(n0)
            if n1 is not None:
                player_name["n1"] = urllib.parse.unquote(n1)
            if n2 is not None:
                player_name["n2"] = urllib.parse.unquote(n2)
            if n3 is not None:
                player_name["n3"] = urllib.parse.unquote(n3)
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
            kaze_table = [
                [0, 1, 2, 3],
                [3, 0, 1, 2],
                [2, 3, 0, 1],
                [1, 2, 3, 0]]
            oya = int(child.attrib["oya"])
            kan = False
            reach = False
            doras.append(dora)
            kyoku_id = int(time.time() * 1000)
            scores = list(map(lambda x: int(x) * 100, child.attrib["ten"].split(",")))
            action_count = 0

            kyokus.append({
                "id": kyoku_id,
                "game_id": game_id,
                "kyoku_num": kyoku_num,
                "honba": honba,
                "reachbou": reachbou,
                "scores": scores,
                "kazes": kaze_table[oya],
            })

            # 配牌
            for n in range(4):
                haistr = child.attrib.get(f"hai{n}")

                if haistr is not None and haistr != "":
                    haipai = num_to_hai(
                        list(map(lambda x: int(x), haistr.split(","))),
                        has_aka)
                    haipais.append({
                        "kyoku_id": kyoku_id,
                        "player_index": n,
                        "haipai": haipai
                    })
        elif child.tag == "DORA":
            _ = 0
        elif child.tag == "REACH":
            reach = child.attrib["step"] == "1"
        elif child.tag == "AGARI":
            ten = child.attrib["ten"].split(",")
            sc = list(map(lambda x: int(x) * 100, child.attrib["sc"].split(",")))
            yakustr = child.attrib.get("yaku")
            yaku = yakustr.split(",") if yakustr is not None else []
            yaku_names: List[str] = []
            yakumanstr = child.attrib.get("yakuman")
            yakuman = yakumanstr.split(",") if yakumanstr is not None else []
            who = int(child.attrib["who"])
            fromWho = int(child.attrib["fromWho"])
            owari = child.attrib.get("owari") is not None

            machi = int(child.attrib["machi"])

            hai_list = list(filter(lambda x: x != machi,
                                   map(lambda x: int(x),
                                       child.attrib["hai"].split(","))))
            hai_list.append(machi)

            tehais = [num_to_hai(hai_list, has_aka)]

            m = child.attrib.get("m")

            if m is not None:
                ms = m.split(",")
                ms.reverse()
                tehais.extend(
                    list(map(lambda x: nakimentsu(int(x), has_aka)[0], ms)))

            fu = int(ten[0])
            han = 0

            for item in yakuman:
                yaku_names.append(yaku_table[int(item)])
                han = 13

            for ind in range(0, len(yaku), 2):
                yaku_names.append(yaku_table[int(yaku[ind])])
                han += int(yaku[ind+1])

            score = int(ten[1])

            scs = [sc[1], sc[3], sc[5], sc[7]]

            dora_str = dora_hai(list(map(lambda x: int(x), child.attrib["doraHai"].split(","))))

            u = child.attrib.get("doraHaiUra")

            uradoras = list(map(lambda x: int(x), u.split(","))) if u is not None else []

            agaris.append({
                "kyoku_id": kyoku_id,
                "machihai": num_to_hai([machi], has_aka),
                "score": score,
                "fu": fu,
                "han": han,
                "tehai": ",".join(tehais),
                "yaku": yaku_names,
                "dora": dora_str,
                "uradora": dora_hai(uradoras),
                "who": who,
                "by": fromWho,
                "score_diff": scs,
                "owari": owari
            })
        elif child.tag == "RYUUKYOKU":
            sc = list(map(lambda x: int(x) * 100, child.attrib["sc"].split(",")))
            scs = [sc[1], sc[3], sc[5], sc[7]]
            typ = child.attrib.get("type")

            name = nagare_table[typ] if typ is not None and nagare_table.get(typ) is not None else "流局"

            nagares.append({
                "kyoku_id": kyoku_id,
                "name": name,
                "score_diff": scs
            })
        elif child.tag == "N":
            # なき
            who = int(child.attrib["who"])
            paist, atype = nakimentsu(int(child.attrib["m"]), has_aka)

            actions.append({
                "kyoku_id": kyoku_id,
                "player_index": who,
                "seq": action_count,
                "type": atype,
                "pais": paist
            })
            action_count += 1
            if atype == "kan":
                kan = True
        elif re.match(r"^[TUVW]\d+$", child.tag):
            # ツモ
            who = (ord(child.tag[0]) - ord("T") + 4 - oya) % 4
            tsumohai = int(child.tag[1:])
            p = num_to_hai([tsumohai], has_aka)

            typ = "tsumo_k" if kan else "tsumo"

            actions.append({
                "kyoku_id": kyoku_id,
                "player_index": who,
                "seq": action_count,
                "type": typ,
                "pais": p
            })
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
            actions.append({
                "kyoku_id": kyoku_id,
                "player_index": who,
                "seq": action_count,
                "type": "sutehai",
                "pais": p
            })
            action_count += 1
    for name in player_name.values():
        players.add(name)

    for idx, key in enumerate(sorted(player_name.items())):
        game_players.append({
            "game_id": game_id,
            "player_name": key[1],
            "player_index": idx
        })


def save_to_parquet(basedir: str):
    wp = pq.ParquetWriter(os.path.join(basedir, "players.parquet"), schema=Player)
    batch = pa.RecordBatch.from_pandas(pd.DataFrame([{"name": name} for name in players]), schema=Player)
    wp.write_batch(batch)
    wp.close()
    wg = pq.ParquetWriter(os.path.join(basedir, "games.parquet"), schema=Game)
    batch = pa.RecordBatch.from_pandas(pd.DataFrame(games), schema=Game)
    wg.write_batch(batch)
    wg.close()
    wgp = pq.ParquetWriter(os.path.join(basedir, "game_players.parquet"), schema=GamePlayer)
    batch = pa.RecordBatch.from_pandas(pd.DataFrame(game_players), schema=GamePlayer)
    wgp.write_batch(batch)
    wgp.close()
    wk = pq.ParquetWriter(os.path.join(basedir, "kyokus.parquet"), schema=Kyoku)
    batch = pa.RecordBatch.from_pandas(pd.DataFrame(kyokus), schema=Kyoku)
    wk.write_batch(batch)
    wk.close()
    wh = pq.ParquetWriter(os.path.join(basedir, "haipais.parquet"), schema=Haipai)
    batch = pa.RecordBatch.from_pandas(pd.DataFrame(haipais), schema=Haipai)
    wh.write_batch(batch)
    wh.close()
    wa = pq.ParquetWriter(os.path.join(basedir, "actions.parquet"), schema=Action)
    batch = pa.RecordBatch.from_pandas(pd.DataFrame(actions), schema=Action)
    wa.write_batch(batch)
    wa.close()
    if len(agaris) > 0:
        wag = pq.ParquetWriter(os.path.join(basedir, "agaris.parquet"), schema=Agari)
        batch = pa.RecordBatch.from_pandas(pd.DataFrame(agaris), schema=Agari)
        wag.write_batch(batch)
        wag.close()
    if len(nagares) > 0:
        wn = pq.ParquetWriter(os.path.join(basedir, "nagares.parquet"), schema=Nagare)
        batch = pa.RecordBatch.from_pandas(pd.DataFrame(nagares), schema=Nagare)
        wn.write_batch(batch)
        wn.close()
    players.clear()
    games.clear()
    game_players.clear()
    kyokus.clear()
    haipais.clear()
    actions.clear()
    agaris.clear()
    nagares.clear()

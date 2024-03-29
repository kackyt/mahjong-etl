import argparse
from datetime import datetime, timezone
import os
import re
import xml.etree.ElementTree as ET

from tqdm import tqdm
from scrape import parse_document, save_to_parquet

parser = argparse.ArgumentParser(description="tenho mahjong log etl tool")

parser.add_argument("--log-dir", "-L", help="path to log directory", type=str, required=True)
parser.add_argument("--output-dir", "-O", help="transform log and output directory", type=str)

args = parser.parse_args()

# use local dir
DATEPATTERN = re.compile(r"\d{8}")
for dir in os.listdir(args.log_dir):
    path = os.path.join(args.log_dir, dir)
    if DATEPATTERN.match(dir) and os.path.isdir(path):
        print(f"run {dir}")
        dt = datetime.strptime(dir, r"%Y%m%d").replace(tzinfo=timezone.utc)
        seqno = 0
        for file in tqdm(os.listdir(path)):
            filepath = os.path.join(path, file)
            if os.path.isfile(filepath):
                game_id, ext = os.path.splitext(file)
                if ext == ".xml":
                    tree = ET.parse(filepath)
                    seqno = parse_document(tree.getroot(), game_id, dt, seqno)
        if args.output_dir is not None:
            save_to_parquet(args.output_dir, dt)

import argparse
import os
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np

from datetime import datetime, timezone
from tqdm import tqdm
from typing import List


def create_record() -> np.ndarray:
    arr = np.arange(136, dtype=np.int32)
    rng.shuffle(arr)

    return arr


parser = argparse.ArgumentParser(description="random pailist generator")

parser.add_argument("--number", "-n", help="number of output", type=int, required=True)
parser.add_argument("--output-dir", "-O", help="pailist output directory", type=str, required=True)

parser.add_argument("--batch-size", "-b", help="record batch size", type=int, default=1024)

args = parser.parse_args()

Paiyama = pa.schema([pa.field("id", pa.uint64()), pa.field("pai_ids", pa.list_(pa.uint32(), 136))])

dt = datetime.now(timezone.utc)
RANDOM_PAIYAMA_ID_OFFSET = 900_000_000_000
rng = np.random.default_rng()


num_batches = args.number // args.batch_size

dirname = os.path.join(args.output_dir, "paiyamas", f"dt={dt.strftime(r'%Y-%m-%d')}")
os.makedirs(dirname, exist_ok=True)

for batch in tqdm(range(num_batches)):
    paiyama_ids: List[int] = []
    pai_ids: List[np.ndarray] = []

    for seqno in range(args.batch_size):
        paiyama_id = RANDOM_PAIYAMA_ID_OFFSET + int(dt.timestamp() / (24 * 3600)) * 100000 + seqno + args.batch_size * batch
        arr = create_record()
        paiyama_ids.append(paiyama_id)
        pai_ids.append(arr)

    table = pa.Table.from_arrays([pa.array(paiyama_ids), pa.array(pai_ids)], schema=Paiyama)

    filename = f"paiyamas-random-{batch:04d}.parquet"
    filepath = os.path.join(dirname, filename)
    with pq.ParquetWriter(filepath, schema=Paiyama) as writer:
        writer.write_table(table)

import argparse
import os
import pyarrow as pa
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
parser.add_argument(
    "--output-dir", "-O", help="pailist output directory", type=str, required=True
)

parser.add_argument(
    "--batch-size", "-b", help="record batch size", type=int, default=1024
)

args = parser.parse_args()

Haiyama = pa.schema(
    [pa.field("kyoku_id", pa.int64()), pa.field("hai_ids", pa.list_(pa.int32(), 136))]
)

dt = datetime.now(timezone.utc)
RANDOM_KYOKU_ID_OFFSET = 900_000_000_000
rng = np.random.default_rng()


num_batches = args.number // args.batch_size

dirname = os.path.join(args.output_dir, "haiyamas", f"dt={dt.strftime(r'%Y-%m-%d')}")
os.makedirs(dirname, exist_ok=True)

for batch in tqdm(range(num_batches)):
    kyoku_ids: List[int] = []
    hai_ids: List[np.ndarray] = []

    for seqno in range(args.batch_size):
        kyoku_id = (
            RANDOM_KYOKU_ID_OFFSET
            + int(dt.timestamp() / (24 * 3600)) * 100000
            + seqno
            + args.batch_size * batch
        )
        arr = create_record()
        kyoku_ids.append(kyoku_id)
        hai_ids.append(arr)

    table = pa.Table.from_arrays(
        [pa.array(kyoku_ids), pa.array(hai_ids)], schema=Haiyama
    )

    filename = f"haiyamas-random-{batch:04d}.parquet"
    filepath = os.path.join(dirname, filename)
    with pa.OSFile(filepath, "wb") as sink:
        with pa.RecordBatchFileWriter(sink, Haiyama) as writer:
            writer.write_table(table)
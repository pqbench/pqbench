# Sample data attribution

The parquet files under `samples/` are unmodified copies of publicly hosted
open datasets, committed via Git LFS so the repository stays small. `make
samples` verifies them; `git lfs pull` fetches them after a clone that skipped
LFS.

| File | Source | License |
| --- | --- | --- |
| `samples/alltypes_plain.parquet` | apache/parquet-testing — `data/alltypes_plain.parquet` | Apache-2.0 |
| `samples/alltypes_dictionary.parquet` | apache/parquet-testing — `data/alltypes_dictionary.parquet` | Apache-2.0 |
| `samples/nested_lists.snappy.parquet` | apache/parquet-testing — `data/nested_lists.snappy.parquet` | Apache-2.0 |
| `samples/nested_maps.snappy.parquet` | apache/parquet-testing — `data/nested_maps.snappy.parquet` | Apache-2.0 |
| `samples/nested_structs.rust.parquet` | apache/parquet-testing — `data/nested_structs.rust.parquet` | Apache-2.0 |
| `samples/delta_byte_array.parquet` | apache/parquet-testing — `data/delta_byte_array.parquet` | Apache-2.0 |
| `samples/squad.parquet` | HuggingFace `rajpurkar/squad` — `plain_text/train/0.parquet` | CC BY-SA 4.0 |

Sources:

- apache/parquet-testing: https://github.com/apache/parquet-testing
- SQuAD: https://huggingface.co/datasets/rajpurkar/squad

Notes:

- The `alltypes_*` and `nested_*` files exercise edge-case types, encodings,
  and nested struct/list/map column paths. `delta_byte_array.parquet` is the
  TPC-DS `customer` table with `DELTA_BYTE_ARRAY` encoding.
- `squad.parquet` is real nested data (a struct column holding a list of
  structs). SQuAD is licensed CC BY-SA 4.0; see the dataset card for details.
- These are unmodified downloads — the bytes are the source's. The `compression`
  subcommand requires NONE-compressed input; only the files stored
  uncompressed can be used with it.

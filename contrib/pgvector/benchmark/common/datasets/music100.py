"""Music100 raw float32 vectors and official MIPS ground-truth adapter."""

from pathlib import Path

import numpy as np

def _ground_truth(root: Path) -> np.ndarray:
    npy = root / "gt.npy"
    if npy.exists():
        return np.load(npy)
    text = root / "groundtruth_music100_top100.txt"
    if text.exists():
        return np.loadtxt(text, dtype=np.int32)
    raise FileNotFoundError(f"missing Music100 ground truth in {root}; run ./download.sh --dataset ip")


def load(data_root: Path, config: dict, query_count: int, k: int,
         validate_expected_size: bool = True):
    from . import VectorDataset
    root = Path(data_root) / "music100"
    base_path = root / "database_music100.bin"
    query_path = root / "query_music100.bin"
    if not base_path.exists() or not query_path.exists():
        raise FileNotFoundError(f"missing Music100 vectors in {root}; run ./download.sh --dataset ip")
    base = np.memmap(base_path, dtype="<f4", mode="r")
    queries = np.fromfile(query_path, dtype="<f4")
    dimension = config["dimension"]
    if base.size % dimension or queries.size % dimension:
        raise ValueError("Music100 vector file size is not divisible by dimension 100")
    dataset = VectorDataset(
        name="music100", metric="ip", dimension=dimension,
        root=root,
        base=base.reshape(-1, dimension),
        queries=queries.reshape(-1, dimension)[:query_count],
        ground_truth=_ground_truth(root)[:query_count, :k],
    )
    return dataset.validate(query_count, k, validate_expected_size)

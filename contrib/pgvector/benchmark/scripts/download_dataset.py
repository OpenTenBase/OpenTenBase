#!/usr/bin/env python3
"""Download and validate the three fixed benchmark datasets."""

import argparse
import json
from pathlib import Path
import shutil
import sys
import tarfile
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from common.config import resolve_metrics

SIFT_URL = "ftp://ftp.irisa.fr/local/texmex/corpus/sift.tar.gz"
GLOVE_URL = "https://ann-benchmarks.com/glove-100-angular.hdf5"
MUSIC_URLS = {
    "database_music100.bin": "https://yadi.sk/d/F85at7Df685hGA",
    "query_music100.bin": "https://yadi.sk/d/pdrfKf1dFkNAQA",
    "groundtruth_music100_top100.txt": "https://yadi.sk/i/RJ_QGT9a1Ih5lw",
}


def require_size(path: Path, expected: int) -> None:
    actual = path.stat().st_size if path.exists() else 0
    if actual != expected:
        raise ValueError(f"{path}: expected {expected} bytes, got {actual}; remove the incomplete file and retry")


def download(url: str, target: Path) -> None:
    if target.is_file() and target.stat().st_size:
        print(f"using {target}")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".part")
    request = Request(url, headers={"User-Agent": "pgvector-baseline/1.0"})
    print(f"downloading {url} -> {target}", flush=True)
    with urlopen(request) as source, partial.open("wb") as destination:
        shutil.copyfileobj(source, destination, length=1024 * 1024)
    partial.replace(target)


def yandex_download(public_url: str, target: Path) -> None:
    if target.is_file() and target.stat().st_size:
        print(f"using {target}")
        return
    api = "https://cloud-api.yandex.net/v1/disk/public/resources/download?" + urlencode({"public_key": public_url})
    with urlopen(Request(api, headers={"User-Agent": "pgvector-baseline/1.0"})) as response:
        href = json.load(response)["href"]
    download(href, target)


def fetch_sift(data_root: Path) -> None:
    root, archive = data_root / "sift1m", data_root / "sift.tar.gz"
    required = ("sift_base.fvecs", "sift_query.fvecs", "sift_groundtruth.ivecs")
    if all((root / name).is_file() for name in required):
        print(f"using {root}")
    else:
        download(SIFT_URL, archive)
        root.mkdir(parents=True, exist_ok=True)
        with tarfile.open(archive, "r:gz") as source:
            for name in required:
                member = source.extractfile(f"sift/{name}")
                if member is None:
                    raise ValueError(f"archive does not contain sift/{name}")
                with member, (root / name).open("wb") as destination:
                    shutil.copyfileobj(member, destination, length=1024 * 1024)
    require_size(root / "sift_base.fvecs", 1_000_000 * (128 + 1) * 4)
    require_size(root / "sift_query.fvecs", 10_000 * (128 + 1) * 4)
    require_size(root / "sift_groundtruth.ivecs", 10_000 * (100 + 1) * 4)


def fetch_glove(data_root: Path) -> None:
    path = data_root / "glove-100-angular" / "glove-100-angular.hdf5"
    download(GLOVE_URL, path)
    import h5py
    with h5py.File(path, "r") as source:
        shapes = {key: source[key].shape for key in ("train", "test", "neighbors")}
    if shapes["train"] != (1_183_514, 100) or shapes["test"] != (10_000, 100):
        raise ValueError(f"unexpected GloVe shapes: {shapes}")
    if shapes["neighbors"][0] != 10_000 or shapes["neighbors"][1] < 10:
        raise ValueError(f"unexpected GloVe ground-truth shape: {shapes['neighbors']}")


def fetch_music(data_root: Path) -> None:
    root = data_root / "music100"
    for filename, url in MUSIC_URLS.items():
        yandex_download(url, root / filename)
    require_size(root / "database_music100.bin", 1_000_000 * 100 * 4)
    require_size(root / "query_music100.bin", 10_000 * 100 * 4)
    truth = np.loadtxt(root / "groundtruth_music100_top100.txt", dtype=np.int32)
    if truth.shape != (10_000, 100):
        raise ValueError(f"unexpected Music100 ground-truth shape: {truth.shape}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", choices=("l2", "cosine", "ip", "all"), default="all")
    parser.add_argument("--data-root", type=Path, required=True)
    args = parser.parse_args()
    actions = {"l2": fetch_sift, "cosine": fetch_glove, "ip": fetch_music}
    for metric in resolve_metrics(args.dataset):
        actions[metric](args.data_root)


if __name__ == "__main__":
    main()

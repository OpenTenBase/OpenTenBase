"""Regenerate and cache exact Top-K when an official full-dataset GT is invalid."""

import argparse
from pathlib import Path

import numpy as np

from .config import resolve_metrics
from .datasets import load_metric


def exact_topk(base: np.ndarray, queries: np.ndarray, metric: str, k: int,
               base_batch: int = 50000, query_batch: int = 16) -> np.ndarray:
    result = np.empty((len(queries), k), dtype=np.int32)
    base_norm = np.linalg.norm(base, axis=1) if metric == "cosine" else None
    base_norm2 = np.einsum("ij,ij->i", base, base) if metric == "l2" else None
    for query_start in range(0, len(queries), query_batch):
        query_block = queries[query_start : query_start + query_batch]
        count = len(query_block)
        best_scores = np.empty((count, 0), dtype=np.float32)
        best_ids = np.empty((count, 0), dtype=np.int32)
        query_norm = np.linalg.norm(query_block, axis=1) if metric == "cosine" else None
        query_norm2 = np.einsum("ij,ij->i", query_block, query_block) if metric == "l2" else None
        for start in range(0, len(base), base_batch):
            block = base[start : start + base_batch]
            if metric == "l2":
                scores = (base_norm2[start : start + len(block), None] + query_norm2[None, :]
                          - 2.0 * (block @ query_block.T)).T
            elif metric == "ip":
                scores = -(block @ query_block.T).T
            elif metric == "cosine":
                denom = base_norm[start : start + len(block), None] * query_norm[None, :]
                similarity = np.divide(block @ query_block.T, denom,
                                       out=np.zeros((len(block), count), dtype=np.float32), where=denom != 0)
                scores = (1.0 - similarity).T
            else:
                raise ValueError(f"unsupported metric {metric}")
            ids = np.broadcast_to(np.arange(start, start + len(block), dtype=np.int32), (count, len(block)))
            scores = np.concatenate((best_scores, scores), axis=1)
            ids = np.concatenate((best_ids, ids), axis=1)
            take = min(k, scores.shape[1])
            selected = np.argpartition(scores, take - 1, axis=1)[:, :take]
            best_scores = np.take_along_axis(scores, selected, axis=1)
            best_ids = np.take_along_axis(ids, selected, axis=1)
        order = np.argsort(best_scores, axis=1)
        result[query_start : query_start + count] = np.take_along_axis(best_ids, order, axis=1)
        print(f"ground truth {query_start + count}/{len(queries)}", flush=True)
    return result


def truth_for_rows(dataset, metric: str, rows: int, query_count: int, k: int) -> np.ndarray:
    """Use official GT for the complete dataset; recompute it for a prefix subset."""
    if rows == dataset.size:
        return dataset.ground_truth[:query_count, :k]
    cache = (dataset.root / "ground_truth" /
             f"{dataset.name}_{metric}_rows{rows}_top{k}_q{query_count}.npy")
    if cache.exists():
        return np.load(cache)
    truth = exact_topk(dataset.base[:rows], dataset.queries[:query_count], metric, k)
    cache.parent.mkdir(parents=True, exist_ok=True)
    np.save(cache, truth)
    return truth


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--metric", choices=("l2", "cosine", "ip", "all"), required=True)
    parser.add_argument("--data-root", type=Path, required=True)
    parser.add_argument("--rows", type=int, required=True)
    parser.add_argument("--query-count", type=int, default=1000)
    parser.add_argument("--k", type=int, default=10)
    args = parser.parse_args()
    for metric in resolve_metrics(args.metric):
        dataset = load_metric(metric, args.data_root, args.query_count, args.k)
        if args.rows <= 0 or args.rows > dataset.size:
            raise ValueError(f"rows must be between 1 and {dataset.size} for {dataset.name}")
        truth = truth_for_rows(dataset, metric, args.rows, args.query_count, args.k)
        print(f"{dataset.name}: ground truth shape={truth.shape}")


if __name__ == "__main__":
    main()

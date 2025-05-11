"""
Driver: coordinates Map → Shuffle → Reduce with multithreading.
"""
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import List, Tuple, Type, Dict

from utils.file_splitter import line_batches

class MapReduceJob:
    def __init__(self, mapper_cls: Type, reducer_cls: Type,
                 num_workers: int = 4, lines_per_batch: int = 50_000):
        self.mapper_cls = mapper_cls
        self.reducer_cls = reducer_cls
        self.num_workers = num_workers
        self.lines_per_batch = lines_per_batch

    def execute(self, csv_path: Path) -> List[Tuple[str, int]]:
        mapped = self._run_map(csv_path)
        totals = self._run_reduce(mapped)
        return self.reducer_cls.get_top_flyers(totals)

    def _run_map(self, csv_path: Path):
        mapper = self.mapper_cls()
        results: List[Tuple[str, int]] = []
        with open(csv_path, "r", buffering=64 * 1024, encoding="utf-8", errors="ignore") as f:
            with ThreadPoolExecutor(max_workers=self.num_workers) as pool:
                futs = [pool.submit(mapper.map, b)
                        for b in line_batches(f, self.lines_per_batch)]
                for fut in as_completed(futs):
                    results.extend(fut.result())
        return results

    def _run_reduce(self, mapped):
        reducer = self.reducer_cls()
        return reducer.reduce(mapped)

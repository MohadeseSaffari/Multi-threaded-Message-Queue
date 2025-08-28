# stats.py
from __future__ import annotations
import time, threading
from collections import defaultdict
from typing import Dict, Tuple

class Stats:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.worker_processed: Dict[str, int] = defaultdict(int)
        self.pri_time_sum: Dict[int, float] = defaultdict(float)
        self.pri_count: Dict[int, int] = defaultdict(int)
        self.started_at = time.time()

    def record(self, worker: str, priority: int, elapsed_sec: float) -> None:
        with self.lock:
            self.worker_processed[worker] += 1
            self.pri_time_sum[priority] += elapsed_sec
            self.pri_count[priority] += 1

    def snapshot(self) -> Tuple[Dict[str, int], Dict[int, float], Dict[int, int], float]:
        with self.lock:
            return (
                dict(self.worker_processed),
                dict(self.pri_time_sum),
                dict(self.pri_count),
                self.started_at,
            )
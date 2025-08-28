# dashboard.py
from __future__ import annotations
import time, threading
from typing import List
from topic_ext import TopicExt
from stats import Stats

def dashboard_loop(
    topics: List[TopicExt],
    stats: Stats,
    stop_event: threading.Event,
    interval: float = 1.0,
):
    while not stop_event.is_set():
        time.sleep(interval)
        worker_processed, pri_time_sum, pri_count, started = stats.snapshot()
        uptime = time.time() - started

        q_line = " | ".join(f"{tp.name}: {tp.size():>3d}" for tp in topics)
        thr = " | ".join(f"{w}: {c:>4d}" for w, c in sorted(worker_processed.items()))

        prios = sorted(set(list(pri_count.keys()) + [1, 2, 3]))
        avg_parts = []
        for p in prios:
            c = pri_count.get(p, 0)
            s = pri_time_sum.get(p, 0.0)
            avg_ms = (s / c * 1000.0) if c else 0.0
            avg_parts.append(f"P{p}: {avg_ms:6.1f} ms ({c} t)")
        avgs = " | ".join(avg_parts)

        print(f"\n[Dashboard @ {uptime:5.1f}s]")
        print(f" Queues  -> {q_line}")
        print(f" Workers -> {thr if thr else '—'}")
        print(f" Avg exec by priority -> {avgs}")

    # final summary
    worker_processed, pri_time_sum, pri_count, _ = stats.snapshot()
    print("\n=== Final Summary ===")
    total = sum(worker_processed.values())
    print(f"Total tasks processed: {total}")
    for p in sorted(pri_count.keys()):
        c = pri_count[p]
        s = pri_time_sum[p]
        avg_ms = (s / c * 1000.0) if c else 0.0
        print(f"  Priority {p}: count={c}, avg={avg_ms:.1f} ms")
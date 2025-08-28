# consumer.py
from __future__ import annotations
import time, threading
from typing import Callable, Optional
from models import Message
from topic_ext import TopicExt, Subscription
from stats import Stats

# Work-queue consumer
def consumer_loop(
    topic: TopicExt,
    name: str,
    *,
    process: Optional[Callable[[Message], None]] = None,
    stop_event: threading.Event | None = None,
    wait_timeout: float = 0.25,
    drain: bool = True,
):
    if stop_event is None:
        stop_event = threading.Event()
    if process is None:
        process = lambda m: print(f"[{name}] processed: {m.id} | {m.content}")

    while True:
        if stop_event.is_set() and not drain:
            break
        msg = topic.dequeue(block=True, timeout=wait_timeout)
        if msg is None:
            if stop_event.is_set():
                if drain:
                    m2 = topic.dequeue(block=False)
                    if m2 is None:
                        break
                    process(m2)
                else:
                    break
            continue
        process(msg)

# Fan-out consumer
def fanout_consumer_loop(
    topic: TopicExt,
    sub: Subscription,
    name: str,
    *,
    process: Optional[Callable[[Message], None]] = None,
    stop_event: threading.Event | None = None,
    wait_timeout: float = 0.25,
):
    if stop_event is None:
        stop_event = threading.Event()
    if process is None:
        process = lambda m: print(f"[{name}] processed: {m.id} | {m.content}")

    while not stop_event.is_set():
        msg = topic.fanout_dequeue(sub, block=True, timeout=wait_timeout)
        if msg is None:
            continue
        process(msg)

def make_processor(stats: Stats, worker_name: str) -> Callable[[Message], None]:
    def _proc(msg: Message):
        t0 = time.perf_counter()
        time.sleep(max(0, msg.work_ms) / 1000.0)
        elapsed = time.perf_counter() - t0
        stats.record(worker_name, getattr(msg, "priority", 1), elapsed)
    return _proc
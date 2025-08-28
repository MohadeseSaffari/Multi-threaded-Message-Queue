# producer.py
from __future__ import annotations
import time, random, threading
from itertools import count
from models import Message
from topic_ext import TopicExt

def producer_loop(
    topic: TopicExt,
    name: str,
    *,
    id_counter=None,
    sleep_range=(0.02, 0.10),
    stop_event: threading.Event | None = None,
    priority_weights=(0.2, 0.5, 0.3),
    work_ms_range_by_priority={1: (25, 60), 2: (40, 90), 3: (60, 130)},
):
    """Continuously create messages and enqueue them to the topic."""
    if id_counter is None:
        id_counter = count(1)
    if stop_event is None:
        stop_event = threading.Event()

    priorities = [1, 2, 3]
    while not stop_event.is_set():
        msg_id = next(id_counter)
        prio = random.choices(priorities, weights=priority_weights, k=1)[0]
        wmin, wmax = work_ms_range_by_priority[prio]
        wms = random.randint(wmin, wmax)
        msg = Message(id=msg_id, content=f"{name}: payload {msg_id}", priority=prio, work_ms=wms)
        topic.enqueue(msg)
        time.sleep(random.uniform(*sleep_range))
# topic_ext.py
from __future__ import annotations
import threading, time
from typing import Optional, Dict
from models import Message
from linked_queue import LinkedQueue

class Subscription:
    """Per-subscriber queue for fan-out mode."""
    def __init__(self, name: str, capacity: Optional[int] = None) -> None:
        self.name = name
        self.capacity = capacity
        self.queue = LinkedQueue()
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)

    def size(self) -> int:
        with self.lock:
            return len(self.queue)

class TopicExt:
    """
    Topic with optional features:
      - capacity: bounded buffer (work-queue or per-subscriber in fan-out)
      - ttl_seconds: retention policy (expired dropped/garbage-collected)
      - fanout: if True, broadcast to per-subscriber queues
    """
    def __init__(
        self,
        name: str,
        *,
        capacity: Optional[int] = None,
        ttl_seconds: Optional[float] = None,
        fanout: bool = False,
        enable_gc: bool = True,
        gc_interval: float = 0.5,
    ) -> None:
        self.name = name
        self.capacity = capacity
        self.ttl_seconds = ttl_seconds
        self.fanout = fanout

        self._queue = LinkedQueue()
        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

        self._subs: Dict[str, Subscription] = {}
        self._gc_interval = gc_interval
        self._gc_stop = threading.Event()
        self._gc_thread = None
        if enable_gc and self.ttl_seconds is not None:
            self._gc_thread = threading.Thread(target=self._gc_loop, daemon=True)
            self._gc_thread.start()

    # --- public API ---
    def stop_background(self) -> None:
        self._gc_stop.set()
        if self._gc_thread:
            self._gc_thread.join(timeout=1.0)

    # fan-out
    def subscribe(self, consumer_name: str, capacity: Optional[int] = None) -> Subscription:
        if not self.fanout:
            raise RuntimeError("Fan-out disabled for this topic.")
        sub = Subscription(consumer_name, capacity)
        with self._lock:
            if consumer_name in self._subs:
                raise ValueError(f"Subscriber '{consumer_name}' exists")
            self._subs[consumer_name] = sub
        return sub

    def enqueue(self, msg: Message, *, block: bool = True, timeout: Optional[float] = None) -> bool:
        if not self.fanout:
            # Work-queue mode (single queue)
            with self._lock:
                # capacity wait
                if self.capacity is not None:
                    if not block and len(self._queue) >= self.capacity:
                        return False
                    end = None if timeout is None else time.monotonic() + timeout
                    while len(self._queue) >= self.capacity:
                        remaining = None if end is None else max(0.0, end - time.monotonic())
                        if remaining == 0.0:
                            return False
                        if not self._not_full.wait(timeout=remaining):
                            return False

                if self._expired(msg):
                    return True  # silently drop expired

                self._queue.enqueue(msg)
                self._not_empty.notify()
                return True

        # Fan-out: broadcast to all subscribers (respect per-sub capacity)
        with self._lock:
            if not self._subs:
                return True  # nobody listening; treat as success
            end = None if timeout is None else time.monotonic() + timeout
            while True:
                blocked = False
                for sub in self._subs.values():
                    if sub.capacity is not None and len(sub.queue) >= sub.capacity:
                        blocked = True
                        break
                if not blocked:
                    break
                if not block:
                    return False
                remaining = None if end is None else max(0.0, end - time.monotonic())
                if remaining == 0.0:
                    return False
                if not self._not_full.wait(timeout=remaining):
                    return False

            # enqueue to each subscriber
            drop = self._expired(msg)
            for sub in self._subs.values():
                with sub.lock:
                    if not drop:
                        sub.queue.enqueue(msg)
                        sub.not_empty.notify()
            return True

    # consumers (work-queue)
    def dequeue(self, *, block: bool = True, timeout: Optional[float] = None) -> Optional[Message]:
        if self.fanout:
            raise RuntimeError("Use fanout_dequeue in fan-out mode.")
        with self._lock:
            if not block and self._queue.is_empty():
                return None

            end = None if timeout is None else time.monotonic() + timeout
            while self._queue.is_empty():
                if not block:
                    return None
                remaining = None if end is None else max(0.0, end - time.monotonic())
                if remaining == 0.0:
                    return None
                if not self._not_empty.wait(timeout=remaining):
                    return None

            # skip expired heads
            while (not self._queue.is_empty()) and self._expired(self._queue.peek()):
                self._queue.dequeue()
                if self.capacity is not None:
                    self._not_full.notify()

            if self._queue.is_empty():
                return None

            msg = self._queue.dequeue()
            if self.capacity is not None:
                self._not_full.notify()
            return msg

    # consumers (fan-out)
    def fanout_dequeue(self, sub: Subscription, *, block: bool = True, timeout: Optional[float] = None) -> Optional[Message]:
        if not self.fanout:
            raise RuntimeError("fanout_dequeue only valid in fan-out mode.")
        with sub.lock:
            if not block and sub.queue.is_empty():
                return None

            end = None if timeout is None else time.monotonic() + timeout
            while sub.queue.is_empty():
                if not block:
                    return None
                remaining = None if end is None else max(0.0, end - time.monotonic())
                if remaining == 0.0:
                    return None
                if not sub.not_empty.wait(timeout=remaining):
                    return None

            # skip expired
            while (not sub.queue.is_empty()) and self._expired(sub.queue.peek()):
                sub.queue.dequeue()
                self._notify_not_full()

            if sub.queue.is_empty():
                return None

            msg = sub.queue.dequeue()
            self._notify_not_full()
            return msg

    def size(self) -> int:
        if self.fanout:
            with self._lock:
                return sum(s.size() for s in self._subs.values())
        with self._lock:
            return len(self._queue)

    def sizes_by_subscriber(self) -> Dict[str, int]:
        with self._lock:
            return {name: sub.size() for name, sub in self._subs.items()}

    # --- internals ---
    def _expired(self, msg: Message) -> bool:
        if self.ttl_seconds is None:
            return False
        return (time.time() - msg.created_at) > self.ttl_seconds

    def _gc_loop(self):
        while not self._gc_stop.is_set():
            time.sleep(self._gc_interval)
            purged = 0
            if not self.fanout:
                with self._lock:
                    purged += self._queue.remove_if(self._expired)
                    if purged and self.capacity is not None:
                        self._not_full.notify_all()
            else:
                with self._lock:
                    subs = list(self._subs.values())
                for sub in subs:
                    with sub.lock:
                        purged += sub.queue.remove_if(self._expired)
                if purged:
                    self._notify_not_full()

    def _notify_not_full(self):
        with self._lock:
            self._not_full.notify_all()
# main.py
from __future__ import annotations
import threading, time
from topic_ext import TopicExt
from producer import producer_loop
from consumer import consumer_loop, make_processor, fanout_consumer_loop
from dashboard import dashboard_loop
from stats import Stats

def main():
    runtime_sec = 8.0
    stop = threading.Event()
    stats = Stats()

    # ------- Choose mode: Work-queue (default) or Fan-out --------
    # Work-queue topics with capacity + TTL:
    topics = [
        TopicExt("alpha", capacity=120, ttl_seconds=5, fanout=False),
        TopicExt("beta",  capacity=120, ttl_seconds=5, fanout=False),
    ]

    # # Fan-out example (uncomment to try broadcast mode):
    # news = TopicExt("news", fanout=True, ttl_seconds=10)
    # sub_a = news.subscribe("alice", capacity=50)
    # sub_b = news.subscribe("bob", capacity=50)
    # topics = [news]

    threads: list[threading.Thread] = []

    # Producers
    for tp in topics:
        for i in range(2):
            th = threading.Thread(
                target=producer_loop,
                args=(tp, f"prod-{tp.name}-{i+1}"),
                kwargs={"stop_event": stop},
                daemon=True,
            )
            threads.append(th)

    # Consumers (work-queue)
    for tp in topics:
        for j in range(2):
            worker_name = f"cons-{tp.name}-{j+1}"
            th = threading.Thread(
                target=consumer_loop,
                args=(tp, worker_name),
                kwargs={
                    "process": make_processor(stats, worker_name),
                    "stop_event": stop,
                    "drain": True,
                    "wait_timeout": 0.2,
                },
                daemon=True,
            )
            threads.append(th)

    # # Consumers (fan-out) — if using the fan-out example above
    # threads.append(threading.Thread(target=fanout_consumer_loop, args=(news, sub_a, "alice"),
    #                                 kwargs={"stop_event": stop, "wait_timeout": 0.2, "process": make_processor(stats, "alice")}, daemon=True))
    # threads.append(threading.Thread(target=fanout_consumer_loop, args=(news, sub_b, "bob"),
    #                                 kwargs={"stop_event": stop, "wait_timeout": 0.2, "process": make_processor(stats, "bob")}, daemon=True))

    # Dashboard
    dash = threading.Thread(target=dashboard_loop, args=(topics, stats, stop), kwargs={"interval": 1.0}, daemon=True)

    # Start
    for th in threads:
        th.start()
    dash.start()

    # Run system
    time.sleep(runtime_sec)

    # Stop and join
    stop.set()
    for th in threads:
        th.join(timeout=2.0)
    dash.join(timeout=2.0)

    # Stop GC threads
    for tp in topics:
        tp.stop_background()

if __name__ == "__main__":
    main()
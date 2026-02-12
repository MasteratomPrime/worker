import logging
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from db import fetch_one_event, mark_event_processed, release_stale_in_progress_events

WORKERS = int(os.environ.get("WORKERS", "4"))
POLL_SLEEP_S = float(os.environ.get("POLL_SLEEP_S", "0.25"))
NO_EVENT_LOG_EVERY_S = float(os.environ.get("NO_EVENT_LOG_EVERY_S", "10"))

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(threadName)s %(message)s",
)
logger = logging.getLogger("worker")

GUARD_INTERVAL_S = 1 * 60


def stale_guard(stop_event: threading.Event) -> None:
    logger.info(
        "stale-guard start interval_s=%s release_threshold=%s retention=%s",
        GUARD_INTERVAL_S,
        "2 minutes",
        "3 minutes",
    )
    while not stop_event.is_set():
        try:
            released, deleted = release_stale_in_progress_events()
            if released:
                logger.warning("stale-guard released %s events (set in_progress=0)", released)
            if deleted:
                logger.warning("stale-guard deleted %s events (retention)", deleted)
        except Exception:
            logger.exception("stale-guard error")

        stop_event.wait(GUARD_INTERVAL_S)


def worker(worker_id: int, stop_event: threading.Event) -> None:
    logger.info("worker start id=%s", worker_id)
    last_no_event_log = 0.0
    while not stop_event.is_set():
        try:
            event = fetch_one_event()
            if event is None:
                now = time.monotonic()
                if now - last_no_event_log >= NO_EVENT_LOG_EVERY_S:
                    logger.info("no events to process")
                    last_no_event_log = now
                time.sleep(POLL_SLEEP_S)
                continue

            event_id, idempotency_key, event_type, payload = event
            logger.info(
                "fetched event id=%s key=%s type=%s payload=%s",
                event_id,
                idempotency_key,
                event_type,
                payload,
            )

            logger.info("processing event id=%s", event_id)
            time.sleep(2)
            mark_event_processed(event_id)
            logger.info("marked processed=1 for id=%s", event_id)
        except Exception:
            logger.exception("worker error id=%s", worker_id)
            time.sleep(1)


def main() -> None:
    stop_event = threading.Event()
    logger.info("program start, WORKERS=%s POLL_SLEEP_S=%s", WORKERS, POLL_SLEEP_S)

    guard_thread = threading.Thread(
        target=stale_guard, name="stale-guard", args=(stop_event,), daemon=True
    )
    guard_thread.start()

    with ThreadPoolExecutor(max_workers=WORKERS) as executor:
        for i in range(WORKERS):
            executor.submit(worker, i, stop_event)

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("stop (KeyboardInterrupt)")
            stop_event.set()


if __name__ == "__main__":
    main()

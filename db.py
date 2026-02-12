import sqlite3
import threading

conn = sqlite3.connect("/home/atom/Projekty/Protopia/backend/baza.db", check_same_thread=False)
conn.execute("PRAGMA journal_mode=WAL;").fetchone()

_db_lock = threading.Lock()


def fetch_one_event() -> tuple | None:
    with _db_lock:
        try:
            cur = conn.execute(
                """
                UPDATE events
                SET in_progress = 1,
                    processing_date = datetime('now')
                WHERE id = (
                    SELECT id
                    FROM events
                    WHERE processed = 0 AND in_progress = 0
                    LIMIT 1
                )
                RETURNING id, idempotency_key, event_type, payload;
                """
            )
            row = cur.fetchone()
            cur.fetchall()
            cur.close()
            conn.commit()
            return None if row is None else tuple(row)
        except Exception:
            conn.rollback()
            raise


def mark_event_processed(event_id: str) -> None:
    with _db_lock:
        try:
            cur = conn.execute(
                """
                UPDATE events
                SET processed = 1
                WHERE id = ? AND processed = 0;
                """,
                (event_id,),
            )
            cur.close()
            conn.commit()
        except Exception:
            conn.rollback()
            raise


def release_stale_in_progress_events() -> tuple[int, int]:
    with _db_lock:
        try:
            cur_release = conn.execute(
                """
                UPDATE events
                SET in_progress = 0
                WHERE processed = 0
                  AND in_progress = 1
                  AND processing_date IS NOT NULL
                  AND datetime(processing_date) <= datetime('now', '-2 minutes');
                """
            )
            released = cur_release.rowcount
            cur_release.close()

            cur_delete = conn.execute(
                """
                DELETE FROM events
                WHERE processing_date IS NOT NULL
                  AND datetime(processing_date) <= datetime('now', '-3 minutes');
                """
            )
            deleted = cur_delete.rowcount
            cur_delete.close()

            conn.commit()
            released_n = 0 if released is None else int(released)
            deleted_n = 0 if deleted is None else int(deleted)
            return released_n, deleted_n
        except Exception:
            conn.rollback()
            raise

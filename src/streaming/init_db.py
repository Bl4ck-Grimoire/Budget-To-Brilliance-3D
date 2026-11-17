"""Creates the streaming tables/views and the read-only Grafana account.

    python -m src.streaming.init_db            # create (idempotent)
    python -m src.streaming.init_db --reset    # also empty the stream_* tables (clean demo)

It only touches ``stream_*`` / ``v_stream_*`` objects: the Data Warehouse is never modified.
"""
from __future__ import annotations

import argparse
import logging
import sys

from src.load import db as dw_db
from src.streaming import store


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Initialise the streaming store")
    ap.add_argument("--reset", action="store_true", help="empty the stream_* tables (Data Warehouse untouched)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    dw_db.ensure_database_exists()
    conn = dw_db.get_connection()
    try:
        store.init_schema(conn)
        if args.reset:
            store.reset_streaming_tables(conn)
        store.ensure_grafana_user(conn)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())

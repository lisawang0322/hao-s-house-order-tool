# db.py (FULL REPLACEMENT)
import sqlite3
from pathlib import Path

DB_PATH = Path("data/orders.db")


def get_conn() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH.as_posix(), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def _column_exists(conn: sqlite3.Connection, table: str, column: str) -> bool:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table});")
    cols = [r["name"] for r in cur.fetchall()]
    return column in cols


def _add_column_if_missing(conn: sqlite3.Connection, table: str, column_def: str) -> None:
    """
    column_def example: "packed_quantity INTEGER NOT NULL DEFAULT 0"
    """
    col_name = column_def.split()[0]
    if not _column_exists(conn, table, col_name):
        cur = conn.cursor()
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_def};")
        conn.commit()


def _dedupe_items_by_order_and_name(conn: sqlite3.Connection) -> None:
    """
    Merge duplicates in items so (order_id, name) becomes unique.
    Strategy:
      - Keep one row (MIN(item_id)) as the survivor.
      - quantity = SUM(quantity)
      - packed_quantity = MIN(SUM(packed_quantity), SUM(quantity))
      - price = MAX(price)
      - Delete the other rows.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT order_id, name, COUNT(*) AS cnt
        FROM items
        GROUP BY order_id, name
        HAVING cnt > 1
    """)
    dups = cur.fetchall()
    if not dups:
        return

    for d in dups:
        order_id = d["order_id"]
        name = d["name"]

        cur.execute("""
            SELECT
              MIN(item_id) AS survivor_id,
              SUM(quantity) AS qty_sum,
              SUM(COALESCE(packed_quantity, 0)) AS packed_sum,
              MAX(price) AS price_max
            FROM items
            WHERE order_id = ? AND name = ?
        """, (order_id, name))
        row = cur.fetchone()

        survivor_id = row["survivor_id"]
        qty_sum = int(row["qty_sum"] or 0)
        packed_sum = int(row["packed_sum"] or 0)
        packed_merged = min(packed_sum, qty_sum)
        price_max = row["price_max"]

        cur.execute("""
            UPDATE items
            SET quantity = ?, packed_quantity = ?, price = ?
            WHERE item_id = ?
        """, (qty_sum, packed_merged, price_max, survivor_id))

        cur.execute("""
            DELETE FROM items
            WHERE order_id = ? AND name = ? AND item_id <> ?
        """, (order_id, name, survivor_id))

    conn.commit()


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    # Orders table (includes is_delivered)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
      order_id TEXT PRIMARY KEY,
      customer TEXT NOT NULL,
      total_dollar REAL,
      is_paid INTEGER NOT NULL DEFAULT 0,
      wants_delivery INTEGER NOT NULL DEFAULT 0,
      is_fulfilled INTEGER NOT NULL DEFAULT 0,
      is_delivered INTEGER NOT NULL DEFAULT 0,
      created_at TEXT DEFAULT (datetime('now'))
    );
    """)

    # Items table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS items (
      item_id TEXT PRIMARY KEY,
      order_id TEXT NOT NULL,
      name TEXT NOT NULL,
      quantity INTEGER NOT NULL,
      price REAL,
      is_checked INTEGER NOT NULL DEFAULT 0,
      packed_quantity INTEGER NOT NULL DEFAULT 0,
      FOREIGN KEY(order_id) REFERENCES orders(order_id)
    );
    """)

    # Catalog table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS catalog (
      item_name TEXT PRIMARY KEY,
      unit_price REAL NOT NULL
    );
    """)

    conn.commit()

    # Migration safety
    _add_column_if_missing(conn, "items", "packed_quantity INTEGER NOT NULL DEFAULT 0")
    _add_column_if_missing(conn, "orders", "is_delivered INTEGER NOT NULL DEFAULT 0")
    _add_column_if_missing(conn, "orders", "delivery_address TEXT")
    _add_column_if_missing(conn, "orders", "delivery_distance_miles REAL")
    _add_column_if_missing(conn, "orders", "delivery_fee REAL")
    _add_column_if_missing(conn, "orders", "delivery_distance_computed_at TEXT")
    _add_column_if_missing(conn, "orders", "delivery_distance_source TEXT")
    _add_column_if_missing(conn, "orders", "amount_received REAL DEFAULT 0")
    _add_column_if_missing(conn, "orders", "change_given REAL DEFAULT 0")
    _add_column_if_missing(conn, "orders", "change_status TEXT DEFAULT 'pending'")

    # Enforce uniqueness for (order_id, name)
    _dedupe_items_by_order_and_name(conn)
    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_items_order_name_unique
        ON items(order_id, name);
    """)
    conn.commit()

    # -----------------------------
    # HARD ENFORCEMENT FOR DELIVERED
    # -----------------------------
    # 1) Normalize existing bad data (if any)
    cur.execute("""
        UPDATE orders
        SET is_delivered = 0
        WHERE wants_delivery = 0 AND is_delivered <> 0;
    """)
    conn.commit()

    # 2) Block invalid INSERTs: cannot create delivered=1 when wants_delivery=0
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS trg_orders_delivered_insert_guard
    BEFORE INSERT ON orders
    WHEN NEW.wants_delivery = 0 AND NEW.is_delivered <> 0
    BEGIN
      SELECT RAISE(ABORT, 'Invalid: is_delivered can only be 1 when wants_delivery is 1');
    END;
    """)

    # 3) Block invalid UPDATEs: cannot set delivered=1 if wants_delivery=0
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS trg_orders_delivered_update_guard
    BEFORE UPDATE OF is_delivered ON orders
    WHEN NEW.wants_delivery = 0 AND NEW.is_delivered <> 0
    BEGIN
      SELECT RAISE(ABORT, 'Invalid: cannot set is_delivered=1 when wants_delivery=0');
    END;
    """)

    # 4) Auto-fix: if wants_delivery is changed to 0, force is_delivered back to 0
    cur.execute("""
    CREATE TRIGGER IF NOT EXISTS trg_orders_wants_delivery_force_reset
    AFTER UPDATE OF wants_delivery ON orders
    WHEN NEW.wants_delivery = 0
    BEGIN
      UPDATE orders SET is_delivered = 0 WHERE order_id = NEW.order_id;
    END;
    """)
    conn.commit()

    conn.close()



def wipe_all() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM items;")
    cur.execute("DELETE FROM orders;")
    cur.execute("DELETE FROM catalog;")
    conn.commit()
    conn.close()

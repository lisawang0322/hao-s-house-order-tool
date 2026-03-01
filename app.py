
# app.py (FULL REPLACEMENT)
import uuid
import streamlit as st
import pandas as pd
import requests
import os

from dotenv import load_dotenv

load_dotenv()

from db import init_db, get_conn, wipe_all
from parsing import parse_orders_and_items  # returns: orders_df, items_df, checklist_df, catalog_df, issues_df


# -----------------------------
# Google Distance (Compute Once)
# -----------------------------

def get_setting(name: str, default: str | None = None) -> str | None:
    # Streamlit Cloud / secrets.toml
    try:
        if name in st.secrets:
            return str(st.secrets[name])
    except Exception:
        pass
    # Local / env (from .env via load_dotenv)
    return os.getenv(name, default)

GOOGLE_MAPS_API_KEY = get_setting("GOOGLE_MAPS_API_KEY")
ORIGIN_ADDRESS = get_setting("ORIGIN_ADDRESS", "55 River Oaks Pl, San Jose, CA")

if not GOOGLE_MAPS_API_KEY:
    raise RuntimeError("Missing GOOGLE_MAPS_API_KEY (set in .env locally or Streamlit Secrets in Cloud).")


def compute_distance_miles_google(destination_address: str) -> float:
    if not GOOGLE_MAPS_API_KEY:
        raise RuntimeError("GOOGLE_MAPS_API_KEY not set in .env")

    url = "https://maps.googleapis.com/maps/api/distancematrix/json"

    params = {
        "origins": ORIGIN_ADDRESS,
        "destinations": destination_address,
        "mode": "driving",
        "units": "imperial",
        "key": GOOGLE_MAPS_API_KEY,
    }

    response = requests.get(url, params=params, timeout=10)

    if response.status_code != 200:
        raise RuntimeError(f"Google API error: HTTP {response.status_code}")

    data = response.json()

    if data.get("status") != "OK":
        raise RuntimeError(f"Google API error: {data.get('status')}")

    element = data["rows"][0]["elements"][0]

    if element.get("status") != "OK":
        raise RuntimeError(f"Distance lookup failed: {element.get('status')}")

    distance_text = element["distance"]["text"]  # e.g. "3.4 mi"
    miles = float(distance_text.replace(" mi", "").replace(",", ""))

    return miles


# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(page_title="Order Checklist", layout="wide")
init_db()
st.title("Orders")

def delivery_fee_from_miles(miles: float) -> float:
    if miles < 2: return 1.99
    if miles < 5: return 2.99
    if miles < 10: return 4.99
    if miles < 20: return 6.99
    return 9.99

def needs_distance_recalc(existing_address: str | None, new_address: str, existing_miles: float | None) -> bool:
    if not new_address.strip():
        return False
    if existing_miles is None:
        return True
    if existing_address is None:
        return True
    return existing_address.strip() != new_address.strip()

@st.dialog("New order")
def new_order_modal():
    customer = st.text_input("Customer name", placeholder="e.g. Chocolaty")
    wants_delivery = st.checkbox("Requires delivery", value=False)
    is_paid = st.checkbox("Paid", value=False)

    # Optional: you can collect notes too (only if you add a column later)
    # notes = st.text_area("Notes (optional)")

    c1, c2 = st.columns(2)
    if c1.button("Create order", type="primary", disabled=(not customer.strip())):
        conn = get_conn()
        try:
            order_id = str(uuid.uuid4())
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO orders (
                    order_id, customer, total_dollar,
                    is_paid, wants_delivery, is_fulfilled, is_delivered
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    order_id,
                    customer.strip(),
                    0.0,
                    1 if is_paid else 0,
                    1 if wants_delivery else 0,
                    0,
                    0,  # DB trigger already enforces delivered rules
                ),
            )
            conn.commit()
        finally:
            conn.close()

        # Optional UX: auto-open the Add Items modal for this new order
        st.session_state["open_add_modal_for"] = order_id

        st.rerun()

    if c2.button("Cancel"):
        st.rerun()



# -----------------------------
# Small UI helpers
# -----------------------------
def status_pill(label: str, value: bool) -> str:
    bg = "#16a34a" if value else "#6b7280"
    fg = "white"
    text = "True" if value else "False"

    return f"""
    <span style="
        display:inline-flex;
        align-items:center;
        padding:2px 10px;
        border-radius:999px;
        background:{bg};
        color:{fg};
        font-size:12px;
        margin-right:8px;
        white-space:nowrap;
    ">
      {label}: {text}
    </span>
    """

def sync_packed_widget_state_from_db(order_id: str) -> None:
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT item_id, quantity, packed_quantity FROM items WHERE order_id = ?", (order_id,))
        rows = cur.fetchall()
        for r in rows:
            item_id = r["item_id"]
            qty = int(r["quantity"])
            packed = int(r["packed_quantity"] or 0)
            st.session_state[f"packed_{item_id}"] = min(packed, qty)
            st.session_state[f"qty_{item_id}"] = qty
    finally:
        conn.close()



# -----------------------------
# DB logic helpers
# -----------------------------
def recompute_order_total(conn, order_id: str) -> None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(SUM(quantity * COALESCE(price, 0)), 0) AS total
        FROM items
        WHERE order_id = ?
        """,
        (order_id,),
    )
    total = float(cur.fetchone()["total"] or 0.0)
    cur.execute("UPDATE orders SET total_dollar = ? WHERE order_id = ?", (total, order_id))
    conn.commit()



def recompute_order_fulfilled(conn, order_id: str) -> None:
    """
    Order fulfilled iff it has at least one item AND every item has packed_quantity >= quantity.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
          COUNT(*) AS total_items,
          SUM(CASE WHEN packed_quantity >= quantity THEN 1 ELSE 0 END) AS fulfilled_items
        FROM items
        WHERE order_id = ?
        """,
        (order_id,),
    )
    row = cur.fetchone()
    total_items = row["total_items"] or 0
    fulfilled_items = row["fulfilled_items"] or 0
    is_fulfilled = 1 if (total_items > 0 and fulfilled_items == total_items) else 0
    cur.execute("UPDATE orders SET is_fulfilled = ? WHERE order_id = ?", (is_fulfilled, order_id))
    conn.commit()


def set_all_packed(conn, order_id: str, packed: bool) -> None:
    cur = conn.cursor()
    if packed:
        cur.execute("UPDATE items SET packed_quantity = quantity WHERE order_id = ?", (order_id,))
    else:
        cur.execute("UPDATE items SET packed_quantity = 0 WHERE order_id = ?", (order_id,))
    conn.commit()
    recompute_order_total(conn, order_id)
    recompute_order_fulfilled(conn, order_id)


def upsert_catalog(conn, catalog_df: pd.DataFrame) -> None:
    """
    Upsert catalog item_name -> unit_price into DB.
    """
    cur = conn.cursor()
    for _, r in catalog_df.iterrows():
        name = str(r["item_name"]).strip()
        price = float(r["unit_price"])
        cur.execute(
            """
            INSERT INTO catalog(item_name, unit_price)
            VALUES(?, ?)
            ON CONFLICT(item_name) DO UPDATE SET unit_price=excluded.unit_price
            """,
            (name, price),
        )
    conn.commit()


# -----------------------------
# Sidebar: Import
# -----------------------------
st.sidebar.header("Import")
uploaded = st.sidebar.file_uploader("Upload Excel", type=["xlsx"])
wipe_before = st.sidebar.checkbox("Wipe existing data before import", value=True)

if st.sidebar.button("Import Excel", disabled=(uploaded is None)):
    if wipe_before:
        wipe_all()

    orders_df, items_df, checklist_df, catalog_df, issues_df = parse_orders_and_items(uploaded)

    # Rename to DB schema
    orders_df = orders_df.rename(
        columns={
            "orderId": "order_id",
            "totalDollar": "total_dollar",
            "isPaid": "is_paid",
            "wantsDelivery": "wants_delivery",
            "isFulfilled": "is_fulfilled",
        }
    )
    items_df = items_df.rename(
        columns={
            "itemId": "item_id",
            "orderId": "order_id",
            "isChecked": "is_checked",
        }
    )

    # Ensure delivered column exists (default 0)
    if "is_delivered" not in orders_df.columns:
        orders_df["is_delivered"] = 0

    # Ensure packed_quantity exists on items (default 0)
    if "packed_quantity" not in items_df.columns:
        items_df["packed_quantity"] = 0

    # Booleans -> ints for sqlite
    for c in ["is_paid", "wants_delivery", "is_fulfilled", "is_delivered"]:
        orders_df[c] = orders_df[c].astype(bool).astype(int)
    items_df["is_checked"] = items_df["is_checked"].astype(bool).astype(int)

    conn = get_conn()

    # Upsert catalog
    # Your catalog_df from parser contains columns ["item_name","unit_price",...]
    if not catalog_df.empty and "item_name" in catalog_df.columns and "unit_price" in catalog_df.columns:
        upsert_catalog(conn, catalog_df[["item_name", "unit_price"]])

    # Insert orders/items
    orders_df[["order_id", "customer", "total_dollar", "is_paid", "wants_delivery", "is_fulfilled", "is_delivered"]].to_sql(
        "orders", conn, if_exists="append", index=False
    )
    items_df[["item_id", "order_id", "name", "quantity", "price", "is_checked", "packed_quantity"]].to_sql(
        "items", conn, if_exists="append", index=False
    )

    # Recompute totals + fulfillment after import (in case)
    for oid in orders_df["order_id"].tolist():
        recompute_order_total(conn, oid)
        recompute_order_fulfilled(conn, oid)

    conn.close()

    st.sidebar.success(f"Imported {len(orders_df)} orders, {len(items_df)} items.")
    if not issues_df.empty:
        st.sidebar.warning(f"Parsing warnings: {len(issues_df)}")


# -----------------------------
# Main: Filters
# -----------------------------
conn = get_conn()

colA, colB, colC, colD = st.columns([2, 1, 1, 1])
search = colA.text_input("Search customer", value="")
filter_unfulfilled = colB.checkbox("Only unfulfilled", value=False)
filter_delivery = colC.checkbox("Only wants delivery", value=False)
filter_undelivered = colD.checkbox("Only undelivered", value=False)

query = "SELECT * FROM orders WHERE 1=1"
params = []

if search.strip():
    query += " AND customer LIKE ?"
    params.append(f"%{search.strip()}%")
if filter_unfulfilled:
    query += " AND is_fulfilled = 0"
if filter_delivery:
    query += " AND wants_delivery = 1"
if filter_undelivered:
    query += " AND is_delivered = 0"

query += " ORDER BY created_at DESC"
orders = pd.read_sql_query(query, conn, params=params)

top1, top2 = st.columns([1, 5])
if top1.button("➕ New order"):
    new_order_modal()


# --- Batch / View totals (sum of currently displayed orders) ---
items_total = float(
    pd.to_numeric(orders["total_dollar"], errors="coerce")
      .fillna(0.0)
      .sum()
)

delivery_total = float(
    pd.to_numeric(orders["delivery_fee"], errors="coerce")
      .fillna(0.0)
      .sum()
)

# Calculate total received and changes owed
total_received = 0.0
changes_owed = 0.0
for _, o in orders.iterrows():
    amount_recv = float(o.get("amount_received") or 0.0)
    if amount_recv >= 0.01:
        total_received += amount_recv
    change_owed = float(o.get("change_given") or 0.0)
    if change_owed >= 0.01:
        changes_owed += change_owed

grand_total = items_total + delivery_total


st.markdown("### Batch totals")
b1, b2, b3, b4, b5 = st.columns(5)
b1.metric("Items total", f"${items_total:,.2f}")
b2.metric("Delivery fees", f"${delivery_total:,.2f}")
b3.metric("Grand total", f"${grand_total:,.2f}")
b4.metric("Total received", f"${total_received:,.2f}")
b5.metric("Changes owed", f"${changes_owed:,.2f}")

st.caption(f"Orders in view: {len(orders)}")
st.divider()



st.caption(f"Showing {len(orders)} orders")


# Load catalog once for add-item UI

catalog = pd.read_sql_query("SELECT item_name, unit_price FROM catalog ORDER BY item_name ASC", conn)
catalog_names = catalog["item_name"].tolist()



def add_item_to_order(order_id: str, item_name: str, qty_to_add: int, unit_price: float) -> None:
    """
    If (order_id, name) exists -> increment quantity; else insert.
    Opens its own DB connection to avoid 'closed database' issues on reruns.
    """
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT item_id, quantity, packed_quantity FROM items WHERE order_id = ? AND name = ?",
            (order_id, item_name),
        )
        existing = cur.fetchone()

        if existing:
            new_qty = int(existing["quantity"]) + int(qty_to_add)
            new_packed = min(int(existing["packed_quantity"]), new_qty)
            cur.execute(
                """
                UPDATE items
                SET quantity = ?, price = ?, packed_quantity = ?
                WHERE item_id = ?
                """,
                (new_qty, float(unit_price), new_packed, existing["item_id"]),
            )
        else:
            cur.execute(
                """
                INSERT INTO items(item_id, order_id, name, quantity, price, is_checked, packed_quantity)
                VALUES(?, ?, ?, ?, ?, 0, 0)
                """,
                (str(uuid.uuid4()), order_id, item_name, int(qty_to_add), float(unit_price)),
            )

        conn.commit()
        recompute_order_total(conn, order_id)
        recompute_order_fulfilled(conn, order_id)
    finally:
        conn.close()



# ============================
# SINGLE MODAL ROUTER (REPLACEMENT)
# Ensures only ONE dialog opens per run.
# ============================

# One active modal at a time: None | "add_items" | "delete_order"
if "active_modal" not in st.session_state:
    st.session_state["active_modal"] = None

if "active_modal_order_id" not in st.session_state:
    st.session_state["active_modal_order_id"] = None


def open_modal(modal_name: str, order_id: str):
    st.session_state["active_modal"] = modal_name
    st.session_state["active_modal_order_id"] = order_id
    st.rerun()


def close_modal():
    st.session_state["active_modal"] = None
    st.session_state["active_modal_order_id"] = None
    st.rerun()


@st.dialog("Add items")
def add_items_modal():
    order_id = st.session_state.get("active_modal_order_id")
    if not order_id:
        close_modal()

    if catalog.empty:
        st.warning("No catalog loaded. Import an Excel file to load the product catalog.")
        if st.button("Close"):
            close_modal()
        return

    st.caption(f"Order ID: {order_id}")

    sel_key = f"modal_sel_{order_id}"
    qty_key = f"modal_qty_{order_id}"
    price_key = f"modal_price_{order_id}"
    last_sel_key = f"modal_last_sel_{order_id}"

    selected = st.selectbox("Item", options=catalog_names, key=sel_key)
    default_price = float(catalog.loc[catalog["item_name"] == selected, "unit_price"].iloc[0])

    if last_sel_key not in st.session_state:
        st.session_state[last_sel_key] = selected
    if st.session_state[last_sel_key] != selected:
        st.session_state[price_key] = default_price
        st.session_state[last_sel_key] = selected
    else:
        if price_key not in st.session_state:
            st.session_state[price_key] = default_price

    qty_to_add = st.number_input("Qty to add", min_value=1, step=1, value=1, key=qty_key)
    unit_price = st.number_input("Unit price", min_value=0.0, step=0.5, key=price_key)

    c1, c2 = st.columns([1, 1])
    if c1.button("Add", type="primary"):
        add_item_to_order(order_id, selected, int(qty_to_add), float(unit_price))
        try:
            sync_packed_widget_state_from_db(order_id)
        except Exception:
            pass
        close_modal()

    if c2.button("Cancel"):
        close_modal()


@st.dialog("Confirm Quantity Adjustment")
def confirm_quantity_adjustment_modal():
    item_id = st.session_state.get("qty_adjust_item_id")
    order_id = st.session_state.get("qty_adjust_order_id")
    adjustment_type = st.session_state.get("qty_adjust_type")  # "packed" or "ordered"
    old_value = st.session_state.get("qty_adjust_old_value")
    new_value = st.session_state.get("qty_adjust_new_value")
    item_name = st.session_state.get("qty_adjust_item_name")

    if not all([item_id, order_id, adjustment_type, old_value is not None, new_value is not None]):
        st.warning("Invalid adjustment data.")
        if st.button("Close"):
            close_qty_adjustment_modal()
        return

    adjustment_label = "Packed quantity" if adjustment_type == "packed" else "Ordered quantity"
    st.markdown(f"Adjust **{item_name}** - {adjustment_label}?")
    st.markdown(f"**{old_value}** → **{new_value}**")

    c1, c2 = st.columns(2)
    if c1.button("Confirm", type="primary", key="confirm_qty_adjust"):
        apply_quantity_adjustment(item_id, order_id, adjustment_type, old_value, new_value)
        close_qty_adjustment_modal()
        st.rerun()

    if c2.button("Cancel", key="cancel_qty_adjust"):
        # Reset the widget's session state to the original value
        if item_id and adjustment_type and old_value is not None:
            widget_key = f"packed_{item_id}" if adjustment_type == "packed" else f"qty_{item_id}"
            st.session_state[widget_key] = old_value
        close_qty_adjustment_modal()
        st.rerun()


@st.dialog("Reduce Ordered Quantity")
def confirm_reduce_quantity_modal():
    item_id = st.session_state.get("reduce_qty_item_id")
    order_id = st.session_state.get("reduce_qty_order_id")
    old_value = st.session_state.get("reduce_qty_old_value")
    new_value = st.session_state.get("reduce_qty_new_value")
    item_name = st.session_state.get("reduce_qty_item_name")

    if not all([item_id, order_id, old_value is not None, new_value is not None]):
        st.warning("Invalid adjustment data.")
        if st.button("Close"):
            close_reduce_quantity_modal()
        return

    st.warning(f"Reduce **{item_name}** ordered quantity?")
    st.markdown(f"**{old_value}** → **{new_value}**")

    c1, c2 = st.columns(2)
    if c1.button("Yes, reduce", type="primary", key="confirm_reduce_qty"):
        apply_quantity_adjustment(item_id, order_id, "ordered", old_value, new_value)
        close_reduce_quantity_modal()
        st.rerun()

    if c2.button("Cancel", key="cancel_reduce_qty"):
        # Reset the widget's session state to the original value
        if item_id and old_value is not None:
            st.session_state[f"qty_{item_id}"] = old_value
        close_reduce_quantity_modal()
        st.rerun()


def open_qty_adjustment_modal(item_id: str, order_id: str, adjustment_type: str, old_value: int, new_value: int, item_name: str):
    st.session_state["qty_adjust_item_id"] = item_id
    st.session_state["qty_adjust_order_id"] = order_id
    st.session_state["qty_adjust_type"] = adjustment_type
    st.session_state["qty_adjust_old_value"] = old_value
    st.session_state["qty_adjust_new_value"] = new_value
    st.session_state["qty_adjust_item_name"] = item_name
    st.session_state["active_modal"] = "confirm_qty_adjustment"
    st.rerun()


def close_qty_adjustment_modal():
    st.session_state["qty_adjust_item_id"] = None
    st.session_state["qty_adjust_order_id"] = None
    st.session_state["qty_adjust_type"] = None
    st.session_state["qty_adjust_old_value"] = None
    st.session_state["qty_adjust_new_value"] = None
    st.session_state["qty_adjust_item_name"] = None
    st.session_state["pending_qty_adjustment"] = None
    st.session_state["active_modal"] = None


def open_reduce_quantity_modal(item_id: str, order_id: str, old_value: int, new_value: int, item_name: str):
    st.session_state["reduce_qty_item_id"] = item_id
    st.session_state["reduce_qty_order_id"] = order_id
    st.session_state["reduce_qty_old_value"] = old_value
    st.session_state["reduce_qty_new_value"] = new_value
    st.session_state["reduce_qty_item_name"] = item_name
    st.session_state["active_modal"] = "confirm_reduce_quantity"
    st.rerun()


def close_reduce_quantity_modal():
    st.session_state["reduce_qty_item_id"] = None
    st.session_state["reduce_qty_order_id"] = None
    st.session_state["reduce_qty_old_value"] = None
    st.session_state["reduce_qty_new_value"] = None
    st.session_state["reduce_qty_item_name"] = None
    st.session_state["pending_qty_adjustment"] = None
    st.session_state["active_modal"] = None


def apply_quantity_adjustment(item_id: str, order_id: str, adjustment_type: str, old_value: int, new_value: int):
    conn = get_conn()
    try:
        cur = conn.cursor()
        if adjustment_type == "packed":
            cur.execute("UPDATE items SET packed_quantity = ? WHERE item_id = ?", (int(new_value), item_id))
        elif adjustment_type == "ordered":
            # When adjusting ordered quantity, clamp packed_quantity if needed
            cur.execute("SELECT packed_quantity FROM items WHERE item_id = ?", (item_id,))
            row = cur.fetchone()
            packed_qty = int(row["packed_quantity"]) if row and pd.notna(row["packed_quantity"]) else 0
            clamped_packed = min(packed_qty, int(new_value))
            cur.execute(
                "UPDATE items SET quantity = ?, packed_quantity = ? WHERE item_id = ?",
                (int(new_value), int(clamped_packed), item_id),
            )
        conn.commit()
        recompute_order_total(conn, order_id)
        recompute_order_fulfilled(conn, order_id)
    finally:
        conn.close()
    # Clear the pending adjustment
    st.session_state["pending_qty_adjustment"] = None


@st.dialog("Remove item")
def confirm_remove_item_modal():
    item_id = st.session_state.get("remove_item_id")
    order_id = st.session_state.get("remove_item_order_id")
    item_name = st.session_state.get("remove_item_name")

    if not all([item_id, order_id, item_name]):
        st.warning("Invalid item data.")
        if st.button("Close"):
            close_remove_item_modal()
        return

    st.warning(f"Remove **{item_name}** from this order?")

    c1, c2 = st.columns(2)
    if c1.button("Yes, remove", type="primary", key="confirm_remove_item"):
        apply_remove_item(item_id, order_id)
        close_remove_item_modal()
        st.rerun()

    if c2.button("Cancel", key="cancel_remove_item"):
        close_remove_item_modal()
        st.rerun()


def open_remove_item_modal(item_id: str, order_id: str, item_name: str):
    st.session_state["remove_item_id"] = item_id
    st.session_state["remove_item_order_id"] = order_id
    st.session_state["remove_item_name"] = item_name
    st.session_state["active_modal"] = "confirm_remove_item"
    st.rerun()


def close_remove_item_modal():
    st.session_state["remove_item_id"] = None
    st.session_state["remove_item_order_id"] = None
    st.session_state["remove_item_name"] = None
    st.session_state["active_modal"] = None


def apply_remove_item(item_id: str, order_id: str):
    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("DELETE FROM items WHERE item_id = ?", (item_id,))
        conn.commit()
        recompute_order_total(conn, order_id)
        recompute_order_fulfilled(conn, order_id)
    finally:
        conn.close()


@st.dialog("Remove order")
def confirm_delete_order_modal():
    order_id = st.session_state.get("active_modal_order_id")
    if not order_id:
        close_modal()

    conn = get_conn()
    try:
        cur = conn.cursor()
        cur.execute("SELECT customer, total_dollar FROM orders WHERE order_id = ?", (order_id,))
        row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        st.warning("Order no longer exists.")
        if st.button("Close"):
            close_modal()
        return

    customer = row["customer"]
    total = float(row["total_dollar"] or 0.0)

    st.warning(
        f"Remove this order?\n\n"
        f"**{customer}** | Total: ${total:,.2f}\n\n"
        f"This will delete the order and all items in it."
    )

    c1, c2 = st.columns(2)
    if c1.button("Yes, remove", type="primary"):
        conn = get_conn()
        try:
            cur = conn.cursor()
            cur.execute("BEGIN")
            cur.execute("DELETE FROM items WHERE order_id = ?", (order_id,))
            cur.execute("DELETE FROM orders WHERE order_id = ?", (order_id,))
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

        close_modal()

    if c2.button("Cancel"):
        close_modal()


# ---- IMPORTANT: call only ONE dialog per run, and only here ----
if st.session_state.get("active_modal") == "delete_order":
    confirm_delete_order_modal()
elif st.session_state.get("active_modal") == "add_items":
    add_items_modal()
elif st.session_state.get("active_modal") == "confirm_qty_adjustment":
    confirm_quantity_adjustment_modal()
elif st.session_state.get("active_modal") == "confirm_reduce_quantity":
    confirm_reduce_quantity_modal()
elif st.session_state.get("active_modal") == "confirm_remove_item":
    confirm_remove_item_modal()


# -----------------------------
# Render orders list (row itself expandable + divider between orders)
# -----------------------------
def status_text(label: str, value: bool) -> str:
    return f"🟢 {label}" if value else f"⚪ {label}"



for _, o in orders.iterrows():
    order_id = o["order_id"]
    customer = o["customer"]
    total = o["total_dollar"]

    paid = bool(o["is_paid"])
    wants_delivery = bool(o["wants_delivery"])
    fulfilled = bool(o["is_fulfilled"])
    delivered = bool(o.get("is_delivered", 0))

    total_display = f"${float(total):.2f}" if pd.notna(total) else "—"

    # -----------------------------
    # Header block (cleaned + robust)
    # -----------------------------

    order_id = o["order_id"]
    customer = o["customer"]

    paid = bool(o.get("is_paid", 0))
    wants_delivery = bool(o.get("wants_delivery", 0))
    fulfilled = bool(o.get("is_fulfilled", 0))
    delivered = bool(o.get("is_delivered", 0))

    # Totals: items total is orders.total_dollar; delivery fee stored separately
    items_total = float(o.get("total_dollar") or 0.0)

    # Normalize delivery fields safely (avoid nan)
    d_addr = (o.get("delivery_address") or "").strip()
    d_fee_raw = o.get("delivery_fee", None)
    d_miles_raw = o.get("delivery_distance_miles", None)

    delivery_fee = float(d_fee_raw) if (wants_delivery and pd.notna(d_fee_raw)) else None
    delivery_miles = float(d_miles_raw) if (wants_delivery and pd.notna(d_miles_raw)) else None

    has_delivery_calc = wants_delivery and d_addr != "" and (delivery_fee is not None) and (delivery_miles is not None) and delivery_miles > 0

    
    # Expander label (replace the existing header_label block)
    items_total = float(o.get("total_dollar") or 0.0)
    d_addr = (o.get("delivery_address") or "").strip()
    d_fee_raw = o.get("delivery_fee", None)
    d_miles_raw = o.get("delivery_distance_miles", None)
    memo = (o.get("memo") or "").strip()

    delivery_fee = float(d_fee_raw) if (wants_delivery and pd.notna(d_fee_raw)) else None
    delivery_miles = float(d_miles_raw) if (wants_delivery and pd.notna(d_miles_raw)) else None

    has_delivery_calc = wants_delivery and d_addr != "" and (delivery_fee is not None) and (delivery_miles is not None) and delivery_miles > 0

    # Truncate memo for header display
    memo_part = f" | Memo: {memo[:30]}{'...' if len(memo) > 30 else ''}" if memo else ""

    if wants_delivery:
        if has_delivery_calc:
            grand_total = items_total + delivery_fee
            header_label = (
                f"{customer} | Items: ${items_total:,.2f} | Delivery: ${delivery_fee:,.2f} "
                f"({delivery_miles:.1f} mi) | Grand: ${grand_total:,.2f}{memo_part}"
            )
        else:
            header_label = f"{customer} | Items: ${items_total:,.2f} | Delivery: — | Grand: ${items_total:,.2f}{memo_part}"
    else:
        header_label = f"{customer} | Items: ${items_total:,.2f}{memo_part}"



    # Row layout: left = name, mid = status pills, right = action icons
    h_left, h_mid, h_right = st.columns([3, 2, 0.6])

    h_left.markdown(f"**{customer}**")

    # Status pills (replace existing status pills block)
    paid = bool(o.get("is_paid", 0))
    fulfilled = bool(o.get("is_fulfilled", 0))
    delivered = bool(o.get("is_delivered", 0))
    handed_off = bool(o.get("is_handed_off", 0))

    if wants_delivery:
        s1, s2, s3, s4 = h_mid.columns(4)
        s1.markdown(status_text("Paid", paid))
        s2.markdown(status_text("Delivery", True))
        s3.markdown(status_text("Fulfilled", fulfilled))
        s4.markdown(status_text("Delivered", delivered))
    else:
        s1, s2, s3, s4 = h_mid.columns(4)
        s1.markdown(status_text("Paid", paid))
        s2.markdown(status_text("Delivery", False))
        s3.markdown(status_text("Fulfilled", fulfilled))
        s4.markdown(status_text("Handed off", handed_off))


    # Action icons (two buttons side-by-side)
    b_add, b_del = h_right.columns(2)
    if b_add.button("➕", key=f"open_add_{order_id}", help="Add items"):
        open_modal("add_items", order_id)
    if b_del.button("🗑", key=f"open_del_{order_id}", help="Remove order"):
        open_modal("delete_order", order_id)


    # The row itself is expandable (no separate button)
    with st.expander(header_label, expanded=False):

        # -------------------------
        # Expanded Order Content (Top actions + toggles)
        # -------------------------
        a1, a2, a3 = st.columns([1, 1, 3])

        if a1.button("Mark all packed", key=f"pack_all_{order_id}"):
            set_all_packed(conn, order_id, True)
            sync_packed_widget_state_from_db(order_id)
            st.rerun()

        if a2.button("Clear packed", key=f"clear_pack_{order_id}"):
            set_all_packed(conn, order_id, False)
            sync_packed_widget_state_from_db(order_id)
            st.rerun()


        # Paid toggle always; Delivered toggle only for delivery orders (pickup gets handoff checkbox)
        if wants_delivery:
            t_paid, t_delivered, _ = a3.columns([1, 1, 2])
        else:
            t_paid, t_handoff, _ = a3.columns([1, 1, 2])

        new_paid = t_paid.checkbox("Paid", value=paid, key=f"paid_ctrl_{order_id}")
        if int(new_paid) != int(paid):
            cur = conn.cursor()
            total_due = float(o.get("total_dollar") or 0.0)
            delivery_fee = float(o.get("delivery_fee") or 0.0)
            grand_total = total_due + delivery_fee
            # If marking as paid, set amount_received to grand total; if unpaid, set to 0
            amount_received = grand_total if new_paid else 0.0
            cur.execute(
                "UPDATE orders SET is_paid = ?, amount_received = ? WHERE order_id = ?",
                (1 if new_paid else 0, amount_received, order_id),
            )
            conn.commit()
            # Clear the payment input's session state so it updates from DB
            st.session_state[f"amount_received_{order_id}"] = amount_received
            st.rerun()

        # Delivery / Handed off logic (use distinct keys)
        if wants_delivery:
            delivered_disabled = not fulfilled
            new_delivered = t_delivered.checkbox(
                "Delivered",
                value=delivered,
                disabled=delivered_disabled,
                key=f"delivered_ctrl_{order_id}",
            )
            if int(new_delivered) != int(delivered):
                cur = conn.cursor()
                cur.execute("UPDATE orders SET is_delivered = ? WHERE order_id = ?", (1 if new_delivered else 0, order_id))
                conn.commit()
                st.rerun()
        else:
            handed_disabled = not fulfilled
            new_handed = t_handoff.checkbox(
                "Handed off",
                value=handed_off,
                disabled=handed_disabled,
                key=f"handed_ctrl_{order_id}",
            )
            if int(new_handed) != int(handed_off):
                cur = conn.cursor()
                cur.execute("UPDATE orders SET is_handed_off = ? WHERE order_id = ?", (1 if new_handed else 0, order_id))
                conn.commit()
                st.rerun()

        # -------------------------
        # Packing Checklist
        # -------------------------
        st.subheader("Packing Checklist")

        items = pd.read_sql_query(
            "SELECT * FROM items WHERE order_id = ? ORDER BY name ASC",
            conn,
            params=(order_id,),
        )

        # Check if there's a pending qty adjustment modal to show
        pending_adj = st.session_state.get("pending_qty_adjustment")
        if pending_adj:
            # Show appropriate modal based on adjustment type
            if pending_adj["type"] == "reduce":
                open_reduce_quantity_modal(
                    pending_adj["item_id"],
                    pending_adj["order_id"],
                    pending_adj["old"],
                    pending_adj["new"],
                    pending_adj["name"],
                )
            else:
                # Regular quantity adjustment (packed or ordered increase)
                open_qty_adjustment_modal(
                    pending_adj["item_id"],
                    pending_adj["order_id"],
                    pending_adj["type"],
                    pending_adj["old"],
                    pending_adj["new"],
                    pending_adj["name"],
                )
        
        if items.empty:
            st.info("No items on this order yet.")
        else:
            for _, it in items.iterrows():
                item_id = it["item_id"]
                name = it["name"]
                qty = int(it["quantity"])
                price = it["price"]
                packed_qty = int(it["packed_quantity"]) if pd.notna(it["packed_quantity"]) else 0

                is_done = packed_qty >= qty
                status = "✅" if is_done else "⬜"
                price_display = f"${float(price):.2f}" if pd.notna(price) else "—"

                r1, r2, r3, r4, r5 = st.columns([4, 1, 1.5, 1.5, 1])

                r1.write(f"{status} {name}")
                
                r2.caption("Unit Price")
                r2.write(f"**{price_display}**")

                new_qty = r3.number_input(
                    "Ordered",
                    min_value=1,
                    value=qty,
                    step=1,
                    key=f"qty_{item_id}",
                )

                new_packed = r4.number_input(
                    "Packed",
                    min_value=0,
                    max_value=qty,
                    value=min(packed_qty, qty),
                    step=1,
                    key=f"packed_{item_id}",
                )

                if r5.button("Remove", key=f"rm_{item_id}"):
                    open_remove_item_modal(item_id, order_id, name)
                if int(new_packed) != packed_qty:
                    cur = conn.cursor()
                    cur.execute("UPDATE items SET packed_quantity = ? WHERE item_id = ?", (int(new_packed), item_id))
                    conn.commit()
                    recompute_order_total(conn, order_id)
                    recompute_order_fulfilled(conn, order_id)
                    st.rerun()

                if int(new_qty) != qty:
                    # If reducing ordered quantity, show warning modal
                    if int(new_qty) < qty:
                        st.session_state["pending_qty_adjustment"] = {
                            "item_id": item_id,
                            "order_id": order_id,
                            "type": "reduce",
                            "old": qty,
                            "new": int(new_qty),
                            "name": name,
                        }
                    else:
                        # Increasing quantity, use regular adjustment modal
                        st.session_state["pending_qty_adjustment"] = {
                            "item_id": item_id,
                            "order_id": order_id,
                            "type": "ordered",
                            "old": qty,
                            "new": int(new_qty),
                            "name": name,
                        }
                    st.rerun()


        # -------------------------
        # Delivery Section (INSIDE expander, UNIQUE keys, compute-once)
        # -------------------------
        if wants_delivery:
            st.subheader("Delivery")

            # Load stored values
            conn2 = get_conn()
            try:
                cur2 = conn2.cursor()
                cur2.execute(
                    """
                    SELECT delivery_address,
                        delivery_distance_miles,
                        delivery_fee,
                        delivery_distance_computed_at
                    FROM orders
                    WHERE order_id = ?
                    """,
                    (order_id,),
                )
                row = cur2.fetchone()
            finally:
                conn2.close()

            existing_address = (row["delivery_address"] if row and row["delivery_address"] else "") or ""
            existing_miles = row["delivery_distance_miles"] if row else None
            existing_fee = row["delivery_fee"] if row else None

            # Use value=existing_address (no session_state prefill needed) and scoped keys
            new_address = st.text_input(
                "Delivery address",
                value=existing_address,
                key=f"delivery_addr_expander_{order_id}",
                placeholder="Street, City, CA ZIP",
            )

            colA, colB = st.columns([1, 1])
            calc_clicked = colA.button("Calculate delivery fee", key=f"calc_fee_expander_{order_id}")
            recalc_clicked = colB.button("Recalculate", key=f"recalc_fee_expander_{order_id}")

            # Compute-once rule:
            # Only call Google if wants_delivery=1 AND address non-empty AND
            # (distance missing OR address changed OR user clicks Recalculate)
            def needs_recalc() -> bool:
                if not new_address.strip():
                    return False
                if recalc_clicked:
                    return True
                if existing_miles is None or float(existing_miles or 0) <= 0:
                    return True
                return existing_address.strip() != new_address.strip()

            if calc_clicked or recalc_clicked:
                if not new_address.strip():
                    st.warning("Please enter a delivery address first.")
                elif calc_clicked and not needs_recalc():
                    st.info("Delivery fee already computed for this address (no API call).")
                else:
                    miles = compute_distance_miles_google(new_address.strip())
                    fee = delivery_fee_from_miles(miles)

                    conn3 = get_conn()
                    try:
                        cur3 = conn3.cursor()
                        cur3.execute(
                            """
                            UPDATE orders
                            SET delivery_address = ?,
                                delivery_distance_miles = ?,
                                delivery_fee = ?,
                                delivery_distance_computed_at = datetime('now'),
                                delivery_distance_source = 'google'
                            WHERE order_id = ?
                            """,
                            (new_address.strip(), float(miles), float(fee), order_id),
                        )
                        conn3.commit()
                    finally:
                        conn3.close()

                    st.success(f"Saved: {miles:.2f} miles → ${fee:.2f}")
                    st.rerun()

            # Display stored results (NO API calls)
            if existing_address.strip():
                st.caption(f"Stored address: {existing_address}")
            if existing_miles is not None and float(existing_miles or 0) > 0:
                st.write(f"Stored distance: **{float(existing_miles):.2f} miles**")
            if existing_fee is not None and float(existing_fee or 0) > 0:
                st.write(f"Stored delivery fee: **${float(existing_fee):.2f}**")

            st.divider()
        
        # -------------------------
        # Payment Section
        # -------------------------
        st.subheader("Payment")
        
        # Load stored values
        conn_pay = get_conn()
        try:
            cur_pay = conn_pay.cursor()
            cur_pay.execute(
                """
                SELECT amount_received, change_given
                FROM orders
                WHERE order_id = ?
                """,
                (order_id,),
            )
            payment_row = cur_pay.fetchone()
        finally:
            conn_pay.close()

        existing_amount_received = float(payment_row["amount_received"] or 0.0) if payment_row else 0.0
        
        # Calculate total due (items + delivery)
        total_due = items_total
        if wants_delivery and delivery_fee is not None:
            total_due += delivery_fee

        # Display total due
        st.write(f"**Total due: ${total_due:.2f}**")

        # Input for amount received with confirm button
        with st.form(key=f"payment_form_{order_id}"):
            col_input, col_btn = st.columns([4, 1])
            
            new_amount_received = col_input.number_input(
                "Amount received",
                min_value=0.0,
                step=0.01,
                value=existing_amount_received if existing_amount_received > 0 else None,
                key=f"amount_received_{order_id}",
                format="%.2f"
            )

            # Handle None value (empty input)
            new_amount_received = new_amount_received or 0.0

            # Auto-calculate change
            auto_change = round(new_amount_received - total_due, 2) if new_amount_received > 0 else 0.0
            
            # Display calculated change
            st.write(f"Change: ${auto_change:.2f}")

            # Confirm button to save (form submission via Enter or button click)
            col_btn.form_submit_button("Confirm", key=f"confirm_payment_{order_id}")

        # Check if form was submitted
        if st.session_state.get(f"confirm_payment_{order_id}"):
            if abs(new_amount_received - existing_amount_received) > 0.001:
                conn_upd = get_conn()
                try:
                    cur_upd = conn_upd.cursor()
                    cur_upd.execute(
                        """
                        UPDATE orders
                        SET amount_received = ?, change_given = ?
                        WHERE order_id = ?
                        """,
                        (float(new_amount_received), auto_change, order_id),
                    )
                    conn_upd.commit()
                finally:
                    conn_upd.close()
                st.rerun()
            else:
                st.info("No change to save.")

        

        st.divider()

        # -------------------------
        # Memo Section
        # -------------------------
        st.subheader("Memo")

        conn_memo = get_conn()
        try:
            cur_memo = conn_memo.cursor()
            cur_memo.execute("SELECT memo FROM orders WHERE order_id = ?", (order_id,))
            memo_row = cur_memo.fetchone()
        finally:
            conn_memo.close()

        existing_memo = (memo_row["memo"] or "") if memo_row else ""

        with st.form(key=f"memo_form_{order_id}"):
            new_memo = st.text_area(
                "Notes (delivery time, special requests, etc.)",
                value=existing_memo,
                key=f"memo_input_{order_id}",
                height=100,
                placeholder="e.g. Deliver after 5pm, ring doorbell, no nuts",
            )
            save_memo = st.form_submit_button("Save memo")

        if save_memo:
            conn_memo2 = get_conn()
            try:
                cur_memo2 = conn_memo2.cursor()
                cur_memo2.execute(
                    "UPDATE orders SET memo = ? WHERE order_id = ?",
                    (new_memo.strip() or None, order_id),
                )
                conn_memo2.commit()
            finally:
                conn_memo2.close()
            st.rerun()

        st.divider()
           
    st.divider()

    
conn.close()

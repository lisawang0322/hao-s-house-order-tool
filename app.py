# app.py (FULL REPLACEMENT)
import uuid
import streamlit as st
import pandas as pd

from db import init_db, get_conn, wipe_all
from parsing import parse_orders_and_items  # returns: orders_df, items_df, checklist_df, catalog_df, issues_df



# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(page_title="Order Checklist", layout="wide")
init_db()
st.title("Orders")


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
        SELECT SUM(quantity * COALESCE(price, 0)) AS total
        FROM items
        WHERE order_id = ?
        """,
        (order_id,),
    )
    total = cur.fetchone()["total"]
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
batch_total = float(orders["total_dollar"].fillna(0).sum())

st.markdown(f"### Batch total: ${batch_total:,.2f}")
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
# Add-items modal (NO key=, compatible)
# ============================

# Initialize modal state once
if "open_add_modal_for" not in st.session_state:
    st.session_state["open_add_modal_for"] = None


@st.dialog("Add items")
def add_items_modal():
    """
    Modal UI: select item from catalog, qty, unit price auto-populates.
    Uses st.session_state["open_add_modal_for"] as the target order_id.
    """
    order_id = st.session_state.get("open_add_modal_for")
    if not order_id:
        # Safety: if modal opens without a target, close it
        st.session_state["open_add_modal_for"] = None
        st.rerun()

    if catalog.empty:
        st.warning("No catalog loaded. Import an Excel file to load the product catalog.")
        if st.button("Close"):
            st.session_state["open_add_modal_for"] = None
            st.rerun()
        return

    st.caption(f"Order ID: {order_id}")

    sel_key = f"modal_sel_{order_id}"
    qty_key = f"modal_qty_{order_id}"
    price_key = f"modal_price_{order_id}"
    last_sel_key = f"modal_last_sel_{order_id}"

    selected = st.selectbox("Item", options=catalog_names, key=sel_key)
    default_price = float(catalog.loc[catalog["item_name"] == selected, "unit_price"].iloc[0])

    # Auto-populate price only when selection changes (don’t clobber manual overrides)
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

        st.session_state["open_add_modal_for"] = None
        st.rerun()

    if c2.button("Cancel"):
        st.session_state["open_add_modal_for"] = None
        st.rerun()


if st.session_state.get("open_add_modal_for"):
    add_items_modal()


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

    # Header content: customer + total + horizontal statuses
    #h_left, h_mid = st.columns([3, 2])
    
    h_left, h_mid, h_right = st.columns([3, 2, 0.3])

    h_left.markdown(f"**{customer}**  |  Total: {total_display}")

    if wants_delivery:
        s1, s2, s3, s4 = h_mid.columns(4)
        s1.markdown(status_text("Paid", paid))
        s2.markdown(status_text("Delivery", wants_delivery))
        s3.markdown(status_text("Fulfilled", fulfilled))
        s4.markdown(status_text("Delivered", delivered))
        header_label = f"{customer} | Total: {total_display}"
    else:
        s1, s2, s3 = h_mid.columns(3)
        s1.markdown(status_text("Paid", paid))
        s2.markdown(status_text("Delivery", wants_delivery))
        s3.markdown(status_text("Fulfilled", fulfilled))
        header_label = f"{customer} | Total: {total_display}"
        
    if h_right.button("➕", key=f"open_add_{order_id}", help="Add items to this order"):
        st.session_state["open_add_modal_for"] = order_id
        st.rerun()

    # The row itself is expandable (no separate button)
    with st.expander(header_label, expanded=False):

        # -------------------------
        # Expanded Order Content
        # -------------------------
        a1, a2, a3 = st.columns([1, 1, 3])

        if a1.button("Mark all packed", key=f"pack_all_{order_id}"):
            set_all_packed(conn, order_id, True)
            #sync_packed_widget_state_from_db(conn, order_id)
            sync_packed_widget_state_from_db(order_id)

            st.rerun()

        if a2.button("Clear packed", key=f"clear_pack_{order_id}"):
            set_all_packed(conn, order_id, False)
            #sync_packed_widget_state_from_db(conn, order_id)
            sync_packed_widget_state_from_db(order_id)
            st.rerun()


        # Paid toggle always; Delivered toggle only for delivery orders
        if wants_delivery:
            t_paid, t_delivered, _ = a3.columns([1, 1, 2])
        else:
            t_paid, _ = a3.columns([1, 3])

        new_paid = t_paid.checkbox("Paid", value=paid, key=f"paid_{order_id}")
        if int(new_paid) != int(paid):
            cur = conn.cursor()
            cur.execute("UPDATE orders SET is_paid = ? WHERE order_id = ?", (1 if new_paid else 0, order_id))
            conn.commit()
            st.rerun()

        if wants_delivery:
            delivered_disabled = not fulfilled
            new_delivered = t_delivered.checkbox(
                "Delivered",
                value=delivered,
                disabled=delivered_disabled,
                key=f"del_{order_id}",
            )
            if int(new_delivered) != int(delivered):
                cur = conn.cursor()
                cur.execute(
                    "UPDATE orders SET is_delivered = ? WHERE order_id = ?",
                    (1 if new_delivered else 0, order_id),
                )
                conn.commit()
                st.rerun()

        # --- Packing Checklist ---
        st.subheader("Packing Checklist")

        items = pd.read_sql_query(
            "SELECT * FROM items WHERE order_id = ? ORDER BY name ASC",
            conn,
            params=(order_id,),
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

                r1, r2, r3, r4 = st.columns([5, 2, 2, 1])

                r1.write(f"{status} {name}  | ordered: {qty}  | unit: {price_display}")

                new_packed = r2.number_input(
                    "Packed",
                    min_value=0,
                    max_value=qty,
                    value=min(packed_qty, qty),
                    step=1,
                    key=f"packed_{item_id}",
                )

                new_qty = r3.number_input(
                    "Ordered",
                    min_value=1,
                    value=qty,
                    step=1,
                    key=f"qty_{item_id}",
                )

                # Remove line item
                if r4.button("Remove", key=f"rm_{item_id}"):
                    cur = conn.cursor()
                    cur.execute("DELETE FROM items WHERE item_id = ?", (item_id,))
                    conn.commit()
                    recompute_order_total(conn, order_id)
                    recompute_order_fulfilled(conn, order_id)
                    st.rerun()

                # Persist packed change
                if int(new_packed) != packed_qty:
                    cur = conn.cursor()
                    cur.execute("UPDATE items SET packed_quantity = ? WHERE item_id = ?", (int(new_packed), item_id))
                    conn.commit()
                    recompute_order_total(conn, order_id)
                    recompute_order_fulfilled(conn, order_id)
                    st.rerun()

                # Persist ordered quantity change (clamp packed_quantity if needed)
                if int(new_qty) != qty:
                    cur = conn.cursor()
                    clamped_packed = min(packed_qty, int(new_qty))
                    cur.execute(
                        "UPDATE items SET quantity = ?, packed_quantity = ? WHERE item_id = ?",
                        (int(new_qty), int(clamped_packed), item_id),
                    )
                    conn.commit()
                    recompute_order_total(conn, order_id)
                    recompute_order_fulfilled(conn, order_id)
                    st.rerun()

    st.divider()

conn.close()

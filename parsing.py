import re
import uuid
import math
import pandas as pd
from typing import Tuple, Dict, Optional, List


def parse_item_catalog(
    file_path: str,
    sheet_name: int | str = 0,
    *,
    summary_marker: str = "商品汇总",
    product_header: str = "商品",
    price_header: str = "单价",
    qty_header: str = "数量",
    amount_header: str = "金额",
    total_marker: str = "总计",
) -> Tuple[pd.DataFrame, Dict[str, float]]:
    """
    Parses the bottom 商品汇总 section into an item catalog with unit prices.
    Returns:
      - items_catalog_df: columns [item_name, unit_price, summary_qty, summary_amount]
      - price_map: dict {item_name: unit_price}

    Assumptions (matches your uploaded sheet):
      - The sheet has a row containing `商品汇总`
      - Immediately below are rows for 商品 + 单价 + 数量 + 金额, then item rows, then 总计
      - Item rows have a string in the 商品 column and numeric 单价
    """
    df = pd.read_excel(file_path, sheet_name=sheet_name)

    if df.shape[1] < 2:
        raise ValueError("Expected at least 2 columns in the sheet.")

    col0 = df.columns[0]
    col1 = df.columns[1]
    col2 = df.columns[2] if df.shape[1] > 2 else None
    col3 = df.columns[3] if df.shape[1] > 3 else None

    marker_mask = df[col0].astype(str).str.strip().eq(summary_marker)
    if not marker_mask.any():
        raise ValueError(f"Could not find summary marker row '{summary_marker}' in column '{col0}'.")

    summary_start_idx = int(df.index[marker_mask][0])

    rows = []
    for i in range(summary_start_idx + 1, len(df)):
        name = df.at[i, col0]
        price = df.at[i, col1]

        # Stop at 总计 row
        if isinstance(name, str) and name.strip() == total_marker:
            break

        # Skip header rows
        if isinstance(name, str) and name.strip() in {product_header, summary_marker, total_marker}:
            continue

        # Valid item row
        if isinstance(name, str) and name.strip():
            if isinstance(price, (int, float)) and not pd.isna(price):
                row = {
                    "item_name": name.strip(),
                    "unit_price": float(price),
                    "summary_qty": float(df.at[i, col2]) if col2 is not None and pd.notna(df.at[i, col2]) else None,
                    "summary_amount": float(df.at[i, col3]) if col3 is not None and pd.notna(df.at[i, col3]) else None,
                }
                rows.append(row)

    if not rows:
        raise ValueError("Found summary marker but did not parse any valid item rows with numeric unit prices.")

    items_catalog_df = pd.DataFrame(rows).drop_duplicates(subset=["item_name"], keep="last").reset_index(drop=True)
    price_map = dict(zip(items_catalog_df["item_name"], items_catalog_df["unit_price"]))

    return items_catalog_df, price_map


def normalize_name(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip())


def parse_order_content(
    content: str,
    price_map: Dict[str, float],
    *,
    delivery_prefixes: Tuple[str, ...] = ("选择配送",),
) -> Tuple[bool, List[Dict[str, Optional[float]]], List[str]]:
    """
    Parses an order '内容' cell into:
      - wantsDelivery: bool
      - items: list of dicts {name, quantity, price}
      - warnings: list[str] describing any parse/price issues

    Rules:
      - Split on Chinese comma '，' or ','.
      - Ignore segments containing '总价'.
      - Detect delivery lines like '选择配送...x1' -> wantsDelivery=True and DO NOT create an item.
      - Extract quantity from trailing 'xN'.
      - Resolve price by matching name to price_map (exact after whitespace normalization),
        falling back to substring matching (longest match).
    """
    wants_delivery = False
    items: List[Dict[str, Optional[float]]] = []
    warnings: List[str] = []

    # Normalize price map keys once
    norm_price_map = {normalize_name(k): v for k, v in price_map.items()}

    text = str(content) if content is not None else ""
    parts = re.split(r"[，,]\s*", text)

    for raw_part in parts:
        p = raw_part.strip().strip(" ，,")
        if not p or "总价" in p:
            continue

        # Delivery detection (treat as boolean)
        if any(p.startswith(prefix) for prefix in delivery_prefixes):
            wants_delivery = True
            continue

        # Quantity parsing (must end with xN)
        m = re.search(r"x\s*(\d+)\s*$", p)
        if not m:
            warnings.append(f"Skipped segment without trailing quantity 'xN': {p}")
            continue

        qty = int(m.group(1))
        name = normalize_name(p[: m.start()])

        # Price lookup: exact normalized key
        price = norm_price_map.get(name)

        # Fallback: longest substring match
        if price is None:
            candidates = [(len(k), k) for k in norm_price_map.keys() if k in name or name in k]
            if candidates:
                candidates.sort(reverse=True)
                canonical = candidates[0][1]
                price = norm_price_map[canonical]
                name = canonical  # snap to canonical catalog name
            else:
                warnings.append(f"Missing price match for item: '{name}'")

        items.append({"name": name, "quantity": qty, "price": price})

    return wants_delivery, items, warnings


def parse_orders_and_items(
    file_path: str,
    sheet_name: int | str = 0,
    *,
    orders_header_marker: str = "序号",
    customer_header: str = "姓名",
    content_header: str = "内容",
    summary_marker: str = "商品汇总",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    End-to-end parser for your sheet:
      - Reads item catalog from 商品汇总 section
      - Reads order rows from the sign-up list section (between 序号 header and 商品汇总)
      - Parses each '内容' into items + wantsDelivery boolean
      - Produces:
          orders_df: [orderId, customer, totalDollar, isPaid, wantsDelivery, isFulfilled]
          items_df:  [itemId, orderId, name, quantity, price, isChecked]
          catalog_df: from parse_item_catalog
          issues_df: any warnings per order

    Notes:
      - isPaid defaults False (Excel doesn’t carry it)
      - isFulfilled defaults False (you can derive it from item checkboxes later)
      - totalDollar computed from parsed items when all prices are known; otherwise None
    """
    # 1) Item catalog + price map
    catalog_df, price_map = parse_item_catalog(
        file_path,
        sheet_name=sheet_name,
        summary_marker=summary_marker,
    )

    # 2) Load sheet for orders section parsing
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    if df.shape[1] < 3:
        raise ValueError("Expected at least 3 columns for orders section parsing.")

    col0, col1, col2 = df.columns[0], df.columns[1], df.columns[2]

    # Find orders header row (where first column == 序号)
    header_mask = df[col0].astype(str).str.strip().eq(orders_header_marker)
    if not header_mask.any():
        raise ValueError(f"Could not find orders header marker '{orders_header_marker}'.")

    orders_header_idx = int(df.index[header_mask][0])

    # Find summary start row (商品汇总) to know where orders stop
    summary_mask = df[col0].astype(str).str.strip().eq(summary_marker)
    if not summary_mask.any():
        raise ValueError(f"Could not find summary marker '{summary_marker}' to determine end of orders section.")
    summary_start_idx = int(df.index[summary_mask][0])

    # Orders rows are between header+1 and summary_start_idx-1
    orders_raw = df.iloc[orders_header_idx + 1 : summary_start_idx].copy()
    orders_raw = orders_raw.rename(columns={col0: "seq", col1: "customer", col2: "content"})

    # Keep only rows with customer + content
    orders_raw = orders_raw[orders_raw["customer"].notna() & orders_raw["content"].notna()].copy()

    # 3) Build outputs
    orders_out = []
    items_out = []
    issues_out = []

    for _, row in orders_raw.iterrows():
        order_id = str(uuid.uuid4())
        customer = str(row["customer"]).strip()
        content = str(row["content"])

        wants_delivery, items, warnings = parse_order_content(content, price_map)

        # Compute total when all prices are known
        total = 0.0
        missing_price = False
        for it in items:
            if it["price"] is None or (isinstance(it["price"], float) and math.isnan(it["price"])):
                missing_price = True
            else:
                total += int(it["quantity"]) * float(it["price"])

        orders_out.append(
            {
                "orderId": order_id,
                "customer": customer,
                "totalDollar": None if missing_price else round(total, 2),
                "isPaid": False,
                "wantsDelivery": wants_delivery,
                "isFulfilled": False,
            }
        )

        for it in items:
            items_out.append(
                {
                    "itemId": str(uuid.uuid4()),
                    "orderId": order_id,
                    "name": it["name"],
                    "quantity": int(it["quantity"]),
                    "price": it["price"],
                    "isChecked": False,
                }
            )

        for w in warnings:
            issues_out.append({"orderId": order_id, "customer": customer, "warning": w, "content_sample": content[:150]})

        if not items:
            issues_out.append(
                {"orderId": order_id, "customer": customer, "warning": "No parsed items", "content_sample": content[:150]}
            )

    orders_df = pd.DataFrame(orders_out)
    items_df = pd.DataFrame(items_out)
    issues_df = pd.DataFrame(issues_out)

    # Optional: Checklist view for UI
    checklist_df = items_df.merge(
        orders_df[["orderId", "customer", "wantsDelivery", "isPaid", "isFulfilled", "totalDollar"]],
        on="orderId",
        how="left",
    )[
        [
            "orderId",
            "customer",
            "wantsDelivery",
            "isPaid",
            "isFulfilled",
            "totalDollar",
            "itemId",
            "name",
            "quantity",
            "price",
            "isChecked",
        ]
    ]

    return orders_df, items_df, checklist_df, catalog_df, issues_df


# Example usage:
# orders_df, items_df, checklist_df, catalog_df, issues_df = parse_orders_and_items("/path/to/0207132701_4281.xlsx")
# print(orders_df.head())
# print(items_df.head())
# print(issues_df.head())

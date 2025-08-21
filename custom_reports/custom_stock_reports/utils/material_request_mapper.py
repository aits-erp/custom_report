import frappe, json
from frappe.utils import flt, nowdate
#from erpnext.manufacturing.report.production_planning_report.production_planning_report import execute as run_report
from custom_reports.custom_stock_reports.report.custom_production_planning_report.custom_production_planning_report import execute as run_report

@frappe.whitelist()
def get_material_request_data_from_report(filters=None):
    # Accept string or dict
    if isinstance(filters, str):
        try:
            filters = json.loads(filters)
        except Exception:
            filters = {}
    filters = frappe._dict(filters or {})
    filters.pop("docnames", None)

    columns, data = run_report(filters)

    exclude_fields = {"required_qty", "available_qty", "arrival_qty", "balance_po_qty"}
    wh_columns = [
        (col.get("label"), col.get("fieldname"))
        for col in (columns or [])
        if col.get("fieldname") and col.get("fieldname").endswith("_qty") and col.get("fieldname") not in exclude_fields
    ]

    totals, best_wh = {}, {}

    for row in (data or []):
        item_code = row.get("item_code") or row.get("raw_material_code")
        if not item_code:
            continue

        balance = flt(row.get("balance_po_qty") or 0)
        frappe.log_error("Row Debug", f"{row.get('item_code') or row.get('raw_material_code')} => BalanceQty: {row.get('balance_po_qty')} | Parsed: {balance}")
        if balance <= 0:
            continue

        totals[item_code] = totals.get(item_code, 0) + balance

        picked_wh, picked_score = None, -1.0
        for label, fieldname in wh_columns:
            v = flt(row.get(fieldname) or 0)
            if v > 0 and v > picked_score:
                picked_score, picked_wh = v, label

        if not picked_wh:
            picked_wh = row.get("warehouse")

        if picked_wh and ((item_code not in best_wh) or (picked_score > best_wh[item_code][1])):
            best_wh[item_code] = (picked_wh, picked_score)

    items = []
    for item_code, qty in totals.items():
        if qty <= 0:
            continue
        wh = (best_wh.get(item_code) or (None, None))[0]
        items.append({
            "item_code": item_code,
            "qty": qty,
            "warehouse": wh,
            "schedule_date": filters.get("schedule_date") or nowdate(),
        })

    return {"items": items}


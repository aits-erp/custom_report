# Copyright (c) 2013, Frappe Technologies Pvt. Ltd. and contributors
# For license information, please see license.txt
import frappe
from frappe import _
from pypika import Order
from erpnext.stock.doctype.warehouse.warehouse import get_child_warehouses
from collections import defaultdict
from frappe import _
from frappe.utils import flt

def execute(filters=None):
	return ProductionPlanReport(filters).execute_report()

class ProductionPlanReport:
	def __init__(self, filters=None):
		self.filters = frappe._dict(filters or {})
		self.raw_materials_dict = {}
		self.data = []
		self.parent_qty_map = {}

	def execute_report(self):
		self.bin_details = {}
		self.get_open_orders()
		self.get_raw_materials()
		self.get_item_details()
		self.get_bin_details()
		self.get_purchase_details()
		self.get_po_qty_map()
		# remove: self.get_parent_warehouse_qty_map()
		self.get_parent_warehouses()   # keeps your old naming but build_parent_warehouse_data below will set parent_warehouses properly
		self.build_parent_warehouse_data()
		self.prepare_data()
		self.get_columns()

		return self.columns, self.data

	# helper to add parent-warehouse + PO fields to a row
	def _enrich_row_parent_po_fields(self, row, item_code):
		"""
		Fill parent-warehouse qty columns, arrival_qty (POQty) and balance_po_qty
		- Uses self.parent_warehouses (list of parent warehouse names)
		- Uses self.parent_qty_map which is: { parent_wh: { item_code: qty, ... }, ... }
		- Uses self.po_qty_map which is: { item_code: po_pending_qty, ... }
		- Uses self.purchase_details which is keyed by (item_code, warehouse)
		"""
		# ensure required_qty is numeric
		required_qty = flt(row.get("required_qty") or 0.0)

		total_parent_qty = 0.0
		for parent_wh in getattr(self, "parent_warehouses", []):
			# parent_qty_map stores parent -> { item_code: qty }
			parent_map = self.parent_qty_map.get(parent_wh, {})
			qty = flt(parent_map.get(item_code, 0.0))
			row[frappe.scrub(f"{parent_wh}_qty")] = qty
			total_parent_qty += qty

		# POQty (arrival_qty) from precomputed map
		po_qty = flt(getattr(self, "po_qty_map", {}).get(item_code, 0.0))
		row["arrival_qty"] = po_qty

		# balance = required - sum(parent qtys) - po_qty
		row["balance_po_qty"] = required_qty - total_parent_qty - po_qty

		# earliest arrival_date (if purchase_details has arrival_date entries)
		arrival_dates = []
		for (it, wh), pd in getattr(self, "purchase_details", {}).items():
			if it == item_code and getattr(pd, "arrival_date", None):
				arrival_dates.append(pd.arrival_date)

		if arrival_dates:
			# pick earliest
			row["arrival_date"] = min(arrival_dates)

	def get_parent_warehouses(self):
		self.parent_warehouses = set()
		for warehouse in self.warehouses:
			parent_warehouse = frappe.get_cached_value("Warehouse", warehouse, "parent_warehouse") or warehouse
			self.parent_warehouses.add(parent_warehouse)
		self.parent_warehouses = sorted(filter(None, self.parent_warehouses))

	def get_parent_warehouse_qty_map(self):
		# """Fetch total qty of each item for each parent warehouse"""
		from frappe.utils.nestedset import get_descendants_of
		qty_map = frappe._dict()
		parent_warehouses = frappe.get_all(
			"Warehouse",
			filters={"is_group": 1},
			pluck="name"
		)

		for parent_wh in parent_warehouses:
			child_whs = get_descendants_of("Warehouse", parent_wh)
			if not child_whs:
				continue
			data = frappe.db.get_all(
				"Bin",
				filters={"warehouse": ["in", child_whs]},
				fields=["item_code", "sum(actual_qty) as total_qty"],
				group_by="item_code"
			)

			for d in data:
				qty_map.setdefault(parent_wh, {})[d.item_code] = d.total_qty or 0

		self.parent_qty_map = qty_map

	def get_po_qty_map(self):
		# """
		# Build a mapping of item_code -> total PO qty based on Purchase Orders.
		# Uses filters if provided (company, from_date, to_date).
		# """
		# base condition: only submitted POs
		conditions = ["po.docstatus = 1"]
		params = {}
		if self.filters.get("company"):
			conditions.append("po.company = %(company)s")
			params["company"] = self.filters.get("company")
		# use transaction_date from Purchase Order (adjust field if you want creation_date)
		if self.filters.get("from_date"):
			conditions.append("po.transaction_date >= %(from_date)s")
			params["from_date"] = self.filters.get("from_date")

		if self.filters.get("to_date"):
			conditions.append("po.transaction_date <= %(to_date)s")
			params["to_date"] = self.filters.get("to_date")

		cond_sql = " AND ".join(conditions)
		sql = f"""
			SELECT poi.item_code AS item_code, SUM(poi.qty) AS po_qty
			FROM `tabPurchase Order Item` poi
			JOIN `tabPurchase Order` po ON po.name = poi.parent
			WHERE {cond_sql}
			GROUP BY poi.item_code
		"""
		rows = frappe.db.sql(sql, params, as_dict=True)
		# store as simple dict for quick lookup
		self.po_qty_map = {r.item_code: (r.po_qty or 0) for r in rows}
		#frappe.msgprint(f"PO map sample: {dict(list(self.po_qty_map.items())[:10])}")

	def get_open_orders(self):
		doctype, order_by = self.filters.based_on, self.filters.order_by

		parent = frappe.qb.DocType(doctype)
		query = None
		if doctype == "Work Order":
			query = (
				frappe.qb.from_(parent)
				.select(
					parent.production_item,
					parent.item_name.as_("production_item_name"),
					parent.planned_start_date,
					parent.stock_uom,
					parent.qty.as_("qty_to_manufacture"),
					parent.name,
					parent.bom_no,
					parent.fg_warehouse.as_("warehouse"),
				)
				.where(parent.status.notin(["Completed", "Stopped", "Closed"]))
			)

			if order_by == "Planned Start Date":
				query = query.orderby(parent.planned_start_date, order=Order.asc)
			if self.filters.docnames:
				query = query.where(parent.name.isin(self.filters.docnames))
		else:
			child = frappe.qb.DocType(f"{doctype} Item")
			query = (
				frappe.qb.from_(parent)
				.from_(child)
				.select(
					child.bom_no,
					child.stock_uom,
					child.warehouse,
					child.parent.as_("name"),
					child.item_code.as_("production_item"),
					child.stock_qty.as_("qty_to_manufacture"),
					child.item_name.as_("production_item_name"),
				)
				.where(parent.name == child.parent)
			)
			if self.filters.docnames:
				query = query.where(child.parent.isin(self.filters.docnames))

			if doctype == "Sales Order":
				query = query.select(
					child.delivery_date,
					parent.base_grand_total,
				).where(
					(child.stock_qty > child.produced_qty)
					& (parent.per_delivered < 100.0)
					& (parent.status.notin(["Completed", "Closed"]))
				)

				if order_by == "Delivery Date":
					query = query.orderby(child.delivery_date, order=Order.asc)
				elif order_by == "Total Amount":
					query = query.orderby(parent.base_grand_total, order=Order.desc)

			elif doctype == "Material Request":
				query = query.select(
					child.schedule_date,
				).where(
					(parent.per_ordered < 100)
					& (parent.material_request_type == "Manufacture")
					& (parent.status != "Stopped")
				)

				if order_by == "Required Date":
					query = query.orderby(child.schedule_date, order=Order.asc)
			query = query.where(parent.docstatus == 1)

		if self.filters.company:
			query = query.where(parent.company == self.filters.company)

		self.orders = query.run(as_dict=True)

	def get_raw_materials(self):
		if not self.orders:
			return
		self.warehouses = [d.warehouse for d in self.orders]
		self.item_codes = [d.production_item for d in self.orders]

		if self.filters.based_on == "Work Order":
			work_orders = [d.name for d in self.orders]
			raw_materials = (
				frappe.get_all(
					"Work Order Item",
					fields=[
						"parent",
						"item_code",
						"item_name as raw_material_name",
						"source_warehouse as warehouse",
						"required_qty",
					],
					filters={"docstatus": 1, "parent": ("in", work_orders), "source_warehouse": ("!=", "")},
				)
				or []
			)
			self.warehouses.extend([d.source_warehouse for d in raw_materials])
		else:
			bom_nos = []

			for d in self.orders:
				bom_no = d.bom_no or frappe.get_cached_value("Item", d.production_item, "default_bom")

				if not d.bom_no:
					d.bom_no = bom_no

				bom_nos.append(bom_no)
			bom_item_doctype = (
				"BOM Explosion Item" if self.filters.include_subassembly_raw_materials else "BOM Item"
			)

			bom = frappe.qb.DocType("BOM")
			bom_item = frappe.qb.DocType(bom_item_doctype)
			if self.filters.include_subassembly_raw_materials:
				qty_field = bom_item.qty_consumed_per_unit
			else:
				qty_field = bom_item.qty / bom.quantity

			raw_materials = (
				frappe.qb.from_(bom)
				.from_(bom_item)
				.select(
					bom_item.parent,
					bom_item.item_code,
					bom_item.item_name.as_("raw_material_name"),
					qty_field.as_("required_qty_per_unit"),
				)
				.where((bom_item.parent.isin(bom_nos)) & (bom_item.parent == bom.name) & (bom.docstatus == 1))
			).run(as_dict=True)

		if not raw_materials:
			return

		self.item_codes.extend([d.item_code for d in raw_materials])
		for d in raw_materials:
			if d.parent not in self.raw_materials_dict:
				self.raw_materials_dict.setdefault(d.parent, [])

			rows = self.raw_materials_dict[d.parent]
			rows.append(d)

	def get_item_details(self):
		if not (self.orders and self.item_codes):
			return
		self.item_details = {}
		for d in frappe.get_all(
			"Item Default",
			fields=["parent", "default_warehouse"],
			filters={"company": self.filters.company, "parent": ("in", self.item_codes)},
		):
			self.item_details[d.parent] = d

	def get_bin_details(self):
		"""
		Fetch Bin records for all item_codes involved (no warehouse restriction).
		Populate self.bin_details keyed as (item_code, warehouse) and ensure
		self.warehouses includes all warehouses discovered.
		"""

		if not (self.orders and self.raw_materials_dict):
			return

		#frappe.msgprint(f"BIN DETAILS KEYS SAMPLE: {list(self.bin_details.keys())[:5]}")
		self.mrp_warehouses = []

		# Keep backwards behaviour for MRP filter if provided
		if self.filters.raw_material_warehouse:
			self.mrp_warehouses.extend(get_child_warehouses(self.filters.raw_material_warehouse))
			self.warehouses.extend(self.mrp_warehouses)

		# Fetch all bins for the item_codes (no warehouse restriction)
		bins = frappe.get_all(
			"Bin",
			fields=["warehouse", "item_code", "actual_qty", "ordered_qty", "projected_qty"],
			filters={"item_code": ("in", self.item_codes)},
		)

		found_whs = set()
		for d in bins:
			key = (d.item_code, d.warehouse)
			if key not in self.bin_details:
				self.bin_details[key] = d
			found_whs.add(d.warehouse)

		# Merge discovered warehouses into self.warehouses
		self.warehouses = list(set(self.warehouses or []) | found_whs)

		#frappe.msgprint(f"bins found for items: {len(bins)}; warehouses discovered: {len(found_whs)}")

	def get_purchase_details(self):
			if not (self.orders and self.raw_materials_dict):
				return
			self.purchase_details = {}
			purchased_items = frappe.get_all(
				"Purchase Order Item",
				fields=["item_code", "min(schedule_date) as arrival_date", "qty as arrival_qty", "warehouse"],
				filters={
					"item_code": ("in", self.item_codes),
					"warehouse": ("in", self.warehouses),
					"docstatus": 1,
				},
				group_by="item_code, warehouse",
			)
			for d in purchased_items:
				key = (d.item_code, d.warehouse)
				if key not in self.purchase_details:
					self.purchase_details.setdefault(key, d)

	# def prepare_data(self):
	# 	if not self.orders:
	# 		return
	# 	for order in self.orders:
	# 		# Determine key for raw materials lookup
	# 		key = order.name if self.filters.based_on == "Work Order" else order.bom_no
	# 		if not self.raw_materials_dict.get(key):
	# 			continue

	# 		# 1. Initialize defaults
	# 		order.update({
	# 			"for_warehouse": order.warehouse,
	# 			"available_qty": 0,
	# 		})

	# 		# Preserve raw material code/date if not already set
	# 		if not getattr(order, "raw_material_code", None):
	# 			order.raw_material_code = order.get("item_code")
	# 		if not getattr(order, "delivery_date", None):
	# 			order.delivery_date = order.get("schedule_date")

	# 		# 2. Available qty from bin
	# 		bin_data = self.bin_details.get((order.production_item, order.warehouse)) or {}
	# 		if bin_data and bin_data.get("actual_qty") > 0 and order.qty_to_manufacture:
	# 			order.available_qty = min(order.qty_to_manufacture, bin_data.get("actual_qty"))
	# 			bin_data["actual_qty"] -= order.available_qty
	# 		# 3. Add PO quantities
	# 		po_qty = self.po_qty_map.get(order.production_item, 0)
	# 		order.arrival_qty = po_qty
	# 		order.balance_po_qty = max(order.qty_to_manufacture - po_qty, 0)

	# 		# 4. Add parent warehouse quantities
	# 		for wh in self.parent_warehouses:
	# 			fieldname = frappe.scrub(f"{wh}_qty")
	# 			qty_val = self.parent_qty_map.get(order.production_item, {}).get(wh, 0)
	# 			order[fieldname] = qty_val

	# 		# 5. Update raw materials allocation
	# 		self.update_raw_materials(order, key)

	def prepare_data(self):
		if not self.orders:
			return

		for order in self.orders:
			key = order.name if self.filters.based_on == "Work Order" else order.bom_no
			if not self.raw_materials_dict.get(key):
				continue

			order.update({
				"for_warehouse": order.warehouse,
				"available_qty": 0,
			})

			if not getattr(order, "raw_material_code", None):
				order.raw_material_code = order.get("item_code")
			if not getattr(order, "delivery_date", None):
				order.delivery_date = order.get("schedule_date")

			# Available qty from bin (exact warehouse match)
			bin_data = self.bin_details.get((order.production_item, order.warehouse)) or {}
			if bin_data and bin_data.get("actual_qty") > 0 and order.qty_to_manufacture:
				order.available_qty = min(order.qty_to_manufacture, bin_data.get("actual_qty"))
				bin_data["actual_qty"] -= order.available_qty

			# PO quantities
			po_qty = self.po_qty_map.get(order.production_item, 0)
			order.arrival_qty = po_qty
			order.balance_po_qty = max(order.qty_to_manufacture - po_qty, 0)

			# Parent warehouse quantities
			for wh in self.parent_warehouses:
				fieldname = frappe.scrub(f"{wh}_qty")
				qty_val = self.parent_qty_map.get(order.production_item, {}).get(wh, 0)
				order[fieldname] = qty_val

			self.update_raw_materials(order, key)

	def update_raw_materials(self, data, key):
		"""
		Iterate raw materials for this 'key' and call pick_materials_from_warehouses.
		Preserves the original allocation logic; ensures safe default when raw_materials_dict.get(key) is None.
		"""
		self.index = 0

		# ensure we have an iterable (avoid NoneType)
		raw_materials_for_key = self.raw_materials_dict.get(key) or []

		warehouses = self.mrp_warehouses or []

		for rm in raw_materials_for_key:
			# compute required_qty for non-Work Order flows
			if self.filters.based_on != "Work Order":
				# rm.required_qty_per_unit exists in BOM path; if not present, fall back to rm.required_qty
				rm.required_qty = getattr(rm, "required_qty_per_unit", None) and (rm.required_qty_per_unit * data.qty_to_manufacture) or getattr(rm, "required_qty", 0)

			# determine "warehouses" list (same as original logic)
			if not warehouses:
				warehouses = [data.warehouse]

			if self.filters.based_on == "Work Order" and getattr(rm, "warehouse", None):
				warehouses = [rm.warehouse]
			else:
				item_details = self.item_details.get(rm.item_code)
				if item_details:
					warehouses = [item_details["default_warehouse"]]

			if self.filters.raw_material_warehouse:
				warehouses = get_child_warehouses(self.filters.raw_material_warehouse)
			# remaining qty initialization and allocate from warehouses
			rm.remaining_qty = rm.required_qty
			self.pick_materials_from_warehouses(rm, data, warehouses)

			# If leftover qty needs to be shown with the chosen raw_material_warehouse
			if rm.remaining_qty and self.filters.raw_material_warehouse and rm.remaining_qty != rm.required_qty:
				row = self.get_args()
				rm.warehouse = self.filters.raw_material_warehouse
				rm.required_qty = rm.remaining_qty
				rm.allotted_qty = 0
				row.update(rm)
				# enrich the fallback row with parent/PO columns as well
				self._enrich_row_parent_po_fields(row, rm.item_code)
				self.data.append(row)
			
	# updated pick_materials_from_warehouses (adds enrichment before append)
	def pick_materials_from_warehouses(self, args, order_data, warehouses):
		"""
		This function largely preserves your existing logic, but before appending
		each row to self.data we call _enrich_row_parent_po_fields(row, item_code).
		"""
		for index, warehouse in enumerate(warehouses):
			if not args.remaining_qty:
				return

			row = self.get_args()

			key = (args.item_code, warehouse)
			bin_data = self.bin_details.get(key)

			if bin_data:
				# copy bin fields (actual_qty, ordered_qty, projected_qty)
				row.update(bin_data)

			args.allotted_qty = 0
			if bin_data and bin_data.get("actual_qty") > 0:
				args.allotted_qty = (
					bin_data.get("actual_qty") if (args.required_qty > bin_data.get("actual_qty")) else args.required_qty
				)
				args.remaining_qty -= args.allotted_qty
				bin_data["actual_qty"] -= args.allotted_qty

			if (self.mrp_warehouses and (args.allotted_qty or index == len(warehouses) - 1)) or not self.mrp_warehouses:
				if not self.index:
					# first time for this order - copy order header fields
					row.update(order_data)
					self.index += 1

				args.warehouse = warehouse
				row.update(args)

				# merge any purchase-details (arrival_date, arrival_qty for this warehouse)
				if self.purchase_details.get(key):
					row.update(self.purchase_details.get(key))

				# --- NEW: add parent warehouse columns, POQty and balance ---
				self._enrich_row_parent_po_fields(row, args.item_code)

				self.data.append(row)

	def get_args(self):
		return frappe._dict(
			{
				"work_order": "",
				"sales_order": "",
				"production_item": "",
				"production_item_name": "",
				"qty_to_manufacture": "",
				"produced_qty": "",
			}
		)
	
	def build_parent_warehouse_data(self):
		"""
		Build:
		- self.parent_warehouses: list of parent names
		- self.parent_qty_map: { parent_wh_name: { item_code: qty, ... }, ... }
		"""
		warehouses = frappe.get_all("Warehouse", fields=["name", "parent_warehouse"])
		wh_map = {w.name: w.parent_warehouse for w in warehouses}

		stock_map = {}  # parent_wh -> { item_code: qty }

		for (item_code, wh), bin_data in (self.bin_details or {}).items():
			qty = flt(bin_data.get("actual_qty", 0))
			if qty <= 0:
				continue

			parent = wh_map.get(wh) or wh
			stock_map.setdefault(parent, {})
			stock_map[parent][item_code] = stock_map[parent].get(item_code, 0) + qty

		self.parent_qty_map = stock_map
		self.parent_warehouses = sorted(stock_map.keys())

		#frappe.msgprint(f"parent_warehouses: {self.parent_warehouses}")
		#frappe.msgprint(f"parent_qty_map sample: {dict(list(self.parent_qty_map.items())[:5])}")

	def get_columns(self):
		based_on = self.filters.based_on

		self.columns = [
			{"label": _("ID"), "options": based_on, "fieldname": "name", "fieldtype": "Link", "width": 100},
			{"label": _("Item Code"), "fieldname": "production_item", "fieldtype": "Link", "options": "Item", "width": 120},
			{"label": _("Item Name"), "fieldname": "production_item_name", "fieldtype": "Data", "width": 130},
			{"label": _("Order Qty"), "fieldname": "qty_to_manufacture", "fieldtype": "Float", "width": 100},
			{"label": _("Available"), "fieldname": "available_qty", "fieldtype": "Float", "width": 100},
		]

		# Add the appropriate date or amount field
		if based_on == "Sales Order" and self.filters.order_by == "Total Amount":
			self.columns.append({
				"label": _("Total Amount"),
				"fieldname": "base_grand_total",
				"fieldtype": "Currency",
				"width": 120
			})
		elif based_on == "Sales Order":
			self.columns.append({
				"label": _("Delivery Date"),
				"fieldname": "delivery_date",
				"fieldtype": "Date",
				"width": 120
			})
		elif based_on == "Material Request":
			self.columns.append({
				"label": _("Required Date"),
				"fieldname": "schedule_date",
				"fieldtype": "Date",
				"width": 120
			})
		elif based_on == "Work Order":
			self.columns.append({
				"label": _("Planned Start Date"),
				"fieldname": "planned_start_date",
				"fieldtype": "Date",
				"width": 120
			})

		# Raw Material Specific Columns
		self.columns.append({
			"label": _("Raw Material Code"),
			"fieldname": "item_code",
			"fieldtype": "Link",
			"options": "Item",
			"width": 120,
		})
		self.columns.append({
			"label": _("Raw Material Name"),
			"fieldname": "raw_material_name",
			"fieldtype": "Data",
			"width": 130,
		})
		self.columns.append({
			"label": _("Required Qty"),
			"fieldname": "required_qty",
			"fieldtype": "Float",
			"width": 100,
		})

		# Add each parent warehouse as its own column with only Qty
		for wh in self.parent_warehouses:
			self.columns.append({
				"label": _(f"{wh}"),
				"fieldname": frappe.scrub(f"{wh}_qty"),
				"fieldtype": "Float",
				"width": 100,
			})

		# Add POQty (from arrival_qty) and calculated BalancePOQty
		self.columns.append({
			"label": _("POQty"),
			"fieldname": "arrival_qty",
			"fieldtype": "Float",
			"width": 100,
		})
		self.columns.append({
			"label": _("BalanceQty"),
			"fieldname": "balance_po_qty",
			"fieldtype": "Float",
			"width": 120,
		})

		return self.columns

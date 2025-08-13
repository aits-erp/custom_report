// Copyright (c) 2025, Aits and contributors
// For license information, please see license.txt

frappe.query_reports["Custom Production Planning Report"] = {
	filters: [
		{
			fieldname: "company",
			label: __("Company"),
			fieldtype: "Link",
			options: "Company",
			reqd: 1,
			default: frappe.defaults.get_user_default("Company"),
		},
		{
			fieldname: "based_on",
			label: __("Based On"),
			fieldtype: "Select",
			options: ["Sales Order", "Material Request", "Work Order"],
			default: "Sales Order",
			reqd: 1,
			on_change: function () {
				let filters = frappe.query_report.filters;
				let based_on = frappe.query_report.get_filter_value("based_on");
				let options = {
					"Sales Order": ["Delivery Date", "Total Amount"],
					"Material Request": ["Required Date"],
					"Work Order": ["Planned Start Date"],
				};

				filters.forEach((d) => {
					if (d.fieldname == "order_by") {
						d.df.options = options[based_on];
						d.set_input(d.df.options);
					}
				});

				frappe.query_report.refresh();
			},
		},
		{
			fieldname: "docnames",
			label: __("Document Name"),
			fieldtype: "MultiSelectList",
			options: "based_on",
			get_data: function (txt) {
				if (!frappe.query_report.filters) return;

				let based_on = frappe.query_report.get_filter_value("based_on");
				if (!based_on) return;

				return frappe.db.get_link_options(based_on, txt);
			},
			get_query: function () {
				var company = frappe.query_report.get_filter_value("company");
				return {
					filters: {
						docstatus: 1,
						company: company,
					},
				};
			},
		},
		{
			fieldname: "raw_material_warehouse",
			label: __("Raw Material Warehouse"),
			fieldtype: "Link",
			options: "Warehouse",
			depends_on: "eval: doc.based_on != 'Work Order'",
			get_query: function () {
				var company = frappe.query_report.get_filter_value("company");
				return {
					filters: {
						company: company,
					},
				};
			},
		},
		{
			fieldname: "order_by",
			label: __("Order By"),
			fieldtype: "Select",
			options: ["Delivery Date", "Total Amount"],
			default: "Delivery Date",
		},
		{
			fieldname: "include_subassembly_raw_materials",
			label: __("Include Sub-assembly Raw Materials"),
			fieldtype: "Check",
			depends_on: "eval: doc.based_on != 'Work Order'",
			default: 0,
		},
	],

	onload: function (report) {
  report.page.add_inner_button(__('+ Create Material Request'), function () {
    // get current filters but don’t force/require docnames
    let filters = frappe.query_report.get_filter_values() || {};
    // We don't want docname filtering for this action
    delete filters.docnames;

    frappe.call({
      method: "custom_reports.custom_stock_reports.utils.material_request_mapper.get_material_request_data_from_report",
      args: { filters: filters },
      callback: function (r) {
        if (r.message && r.message.items && r.message.items.length > 0) {
          frappe.model.with_doctype("Material Request", function () {
            let doc = frappe.model.get_new_doc("Material Request");
            doc.material_request_type = "Purchase";
            if (filters.company) doc.company = filters.company;

            for (let item of r.message.items) {
              let child = frappe.model.add_child(doc, "items");
              child.item_code = item.item_code;
              child.qty = item.qty;                       // <- from BalanceQty
              if (item.schedule_date) child.schedule_date = item.schedule_date;
              if (item.warehouse) child.warehouse = item.warehouse;
            }

            frappe.set_route("Form", "Material Request", doc.name);
          });
        } else {
          frappe.msgprint(__('No items found to create Material Request.'));
        }
      }
    });
  }).css({
    'background-color': 'black',
    'color': 'white',
    'border': '1px solid black'
  });
},

	formatter: function (value, row, column, data, default_formatter) {
		value = default_formatter(value, row, column, data);

		if (
			column.fieldname == "production_item_name" &&
			data &&
			data.qty_to_manufacture > data.available_qty
		) {
			value = `<div style="color:red">${value}</div>`;
		}

		if (column.fieldname == "production_item" && !data.name) {
			value = "";
		}

		if (column.fieldname == "raw_material_name" && data && data.required_qty > data.allotted_qty) {
			value = `<div style="color:red">${value}</div>`;
		}

		return value;
	},
};

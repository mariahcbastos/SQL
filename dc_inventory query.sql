SELECT distinct
  b1.inventory_date
, b1.last_reported
, i2.latest_inventory_date
, case when b1.inventory_date = i2.latest_inventory_date then 'Y' else 'N' end latest_day
, case when b1.inventory_date = date_add('day', -1,date_add('month', 1, date_trunc('month', b1.inventory_date)))then 'Y' else 'N' end  month_end
, t1.trading_partner_type
, t1.is_active dc_active
, t1.trading_partner_name dc_name
, t1.trading_partner_number dc_number
, t1.shipping_city dc_city
, t1.shipping_state dc_state
, t1.shipping_zip dc_zip
, t1.shipping_country dc_country
, t1.shipping_longitude dc_longitude
, t1.shipping_latitude dc_latitude
, t1.ipc_region
, t1.ipc_zone
, t1.ipc_zdm
, t1.ipc_rdm
, t1.gln
, p1.gtin
, p1.product_number
,p1.brand_name
, p1.product_name
, p1.product_group
, p1.category_l1
, p1.category_l2
, p1.category_l3
, p1.category_l4
, p1.category_l5
, p1.product_deleted
, p1.product_country
, p1.manufacturer_number
, p1.manufacturer_name
, i1.dc_product_number
, i1.mfr_product_number
, i1.number_of_stores
, CASE WHEN t1.SHIPPING_COUNTRY = 'CANADA' THEN 'IPC SUBWAY CANADA' ELSE 'IPC SUBWAY USA' END "operator"
, i1.pack
, i1.pack_size
, nvl(i3.contract_delivered_price, 0.0) lcr_delivered_price
, NVL(i4.contract_delivered_price, 0.0) lcr_delivered_price_nm
, NVL(i5.contract_delivered_price, 0.0) lcr_delivered_price_pm
, NVL((i3.contract_delivered_price * i1.total_on_hand_qty), 0.0) inventory_value
, date(DATEADD(d,-DATEPART(dow, b1.inventory_date), b1.inventory_date)) as week_start_date
, NVL(CASE when ((i1.total_wtd_sales_qty = 0.00) OR (i1.total_wtd_sales_qty IS NULL)) THEN sum(i7.total_wtd_sales_qty) ELSE i1.total_wtd_sales_qty END, 0.0) total_wtd_sales_qty
--, NVL(i1.total_wtd_sales_qty, 0.0) total_wtd_sales_qty
--, NVL(i1.week_1_sales, 0.0) week_1_sales
, NVL(CASE when ((i1.week_1_sales = 0.00) OR (i1.week_1_sales IS NULL)) THEN i6.week_1_sales ELSE i1.week_1_sales END, 0.0) week_1_sales
, NVL(i1.week_2_sales, 0.0) week_2_sales--and (dc_week_start_date -6 = i2.current_week))
, NVL(i1.week_3_sales, 0.0) week_3_sales
, NVL(i1.week_4_sales, 0.0) week_4_sales
, NVL(i1.week_5_sales, 0.0) week_5_sales
, NVL(i1.week_6_sales, 0.0) week_6_sales
, NVL(i1.week_7_sales, 0.0) week_7_sales
, NVL(i1.week_8_sales, 0.0) week_8_sales
, NVL(i1.week_9_sales, 0.0) week_9_sales
, NVL(i1.week_10_sales, 0.0) week_10_sales
, NVL(i1.week_11_sales, 0.0) week_11_sales
, NVL(i1.week_12_sales, 0.0) week_12_sales
, NVL(i1.week_13_sales, 0.0) week_13_sales
, NVL(i1.total_on_hand_qty, 0.0) total_on_hand_qty
--, NVL(i1.boh_qty, 0.0) boh_qty
, NVL(CASE when ((i1.boh_qty = 0.00) OR (i1.boh_qty IS NULL)) then (i1.total_on_hand_qty - i1.commit_qty - i1.unavailable_qty) ELSE i1.boh_qty END, 0.0) boh_qty
, NVL(i1.boh_cs_store, 0.0) boh_cs_store
, NVL(i1.commit_qty, 0.0) commit_qty
, NVL(i1.adjustments, 0.0) adjustments
, NVL(i1.unavailable_qty, 0.0) unavailable_qty
, NVL(i1.days_on_hand, 0.0) days_on_hand
, NVL(i1.days_on_hand_on_order, 0.0) days_on_hand_on_order
, NVL(i1.days_until_appt, 0.0) days_until_appt
, NVL(i1.doh_days_until_appt, 0.0) doh_days_until_appt
, NVL(i1.total_on_order, 0.0) total_on_order
, i1.po1_num po1_number
, NVL(i1.po1_qty, 0.0) po1_quantity
, i1.po1_req_date
, i1.po1_dlv_date
, i1.po2_num po2_number
, NVL(i1.po2_qty, 0.0) po2_quantity
, i1.po2_req_date
, i1.po2_dlv_date
, i1.po3_num po3_number
, NVL(i1.po3_qty, 0.0) po3_quantity
, i1.po3_req_date
, i1.po3_dlv_date
, i1.po4_num po4_number
, NVL(i1.po4_qty, 0.0) po4_quantity
, i1.po4_req_date
, i1.po4_dlv_date
, i1.po5_num po5_number
, NVL(i1.po5_qty, 0.0) po5_quantity
, i1.po5_req_date
, i1.po5_dlv_date
, NVL(i1.total_rcvd_wtd, 0.0) total_rcvd_wtd
, NVL(i1.last_rcvd_qty, 0.0) last_rcvd_qty
, i1.last_rcvd_date
, i1.last_rcvd_trx_date
, i1.inventory_status
, i1.max_allowed_qty
, case when t1.shipping_country = 'CANADA' then s1.cad_moving_seasonal_factor else s1.moving_seasonal_factor end moving_seasonal_factor
FROM
  (
   SELECT
     d5.inventory_date
   , d5.trading_partner_number
   , d5.product_number
   , "max"(d6.inventory_date) last_reported
   FROM
     (
      SELECT DISTINCT
        d4.inventory_date
      , d2.trading_partner_number
      , d2.product_number
      FROM
        (
         SELECT DISTINCT
                (CASE WHEN (("im"."extid" IS NULL) OR ("im"."extid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."extid" END) product_number
                , (CASE WHEN (("im"."dcextid" IS NULL) OR ("im"."dcextid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."dcextid" END) trading_partner_number
                , min(inventory_date) starting_inventory_date
        FROM dsipc.dc_inv_po dcinv
        INNER JOIN dsipc.dc_item_mapping im ON "dcinv"."asdcitemid" = "im"."asdcitemid"
        LEFT JOIN dsipc.dc_inventory_delete dinv ON "dcinv"."asdcinventoryid" = "dinv"."asdcinventoryid"
        WHERE "dinv"."asdcinventoryid" IS NULL
         GROUP BY 1,2
      )  d2
      LEFT JOIN (
         SELECT DISTINCT inventory_date, (SELECT max(inventory_date) FROM dsipc.dc_inv_po) latestinventory
         FROM
          dsipc.dc_inv_po
      )  d4 ON d4.inventory_date BETWEEN d2.starting_inventory_date AND   d4.latestinventory
   )  d5
   INNER JOIN (
        SELECT DISTINCT
      (CASE WHEN (("im"."extid" IS NULL) OR ("im"."extid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."extid" END) product_number
      , (CASE WHEN (("im"."dcextid" IS NULL) OR ("im"."dcextid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."dcextid" END) trading_partner_number
      , inventory_date
      FROM
        dsipc.dc_inv_po dcinv
         INNER JOIN dsipc.dc_item_mapping im ON "dcinv"."asdcitemid" = "im"."asdcitemid"
        LEFT JOIN dsipc.dc_inventory_delete dinv ON "dcinv"."asdcinventoryid" = "dinv"."asdcinventoryid"
        WHERE "dinv"."asdcinventoryid" IS NULL
   )  d6 ON d6.inventory_date BETWEEN "date_add"('day', -10, d5.inventory_date) AND d5.inventory_date AND d5.trading_partner_number = d6.trading_partner_number AND d5.product_number = d6.product_number
   GROUP BY d5.inventory_date, d5.trading_partner_number, d5.product_number
)  b1
LEFT JOIN (
   SELECT
    dcinv.asdcitemid, "inventory_status"  , "max_allowed_qty"  , "inventory_date", "firstdayofweek", "total_wtd_sales_qty"
   , dc_week_start_date, week_1_sales , week_2_sales , week_3_sales, week_4_sales , week_5_sales , week_6_sales , week_7_sales
   , week_8_sales, week_9_sales, week_10_sales, week_11_sales , week_12_sales, week_13_sales
   , "quantityreceived", "adjustments", "commit_qty" , "total_on_hand_qty", "unavailable_qty", "boh_qty"
   , boh_cs_store, days_on_hand, days_on_hand_on_order, total_on_order, number_of_stores, dc_product_number
   , dcinv.mfr_product_number, pack, pack_size, po1_num, po1_qty, po1_req_date, po1_dlv_date
   , po2_num, po2_qty, po2_req_date, po2_dlv_date, po3_num, po3_qty, po3_req_date, po3_dlv_date
   , po4_num, po4_qty, po4_req_date, po4_dlv_date, po5_num, po5_qty, po5_req_date, po5_dlv_date
   , total_rcvd_wtd, last_rcvd_qty, last_rcvd_date, last_rcvd_trx_date, days_until_appt, doh_days_until_appt
   , (CASE WHEN (("im"."extid" IS NULL) OR ("im"."extid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."extid" END) "product_number"
   , (CASE WHEN (("im"."dcextid" IS NULL) OR ("im"."dcextid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."dcextid" END) "trading_partner_number"
   FROM dsipc.dc_inv_po dcinv
   INNER JOIN dsipc.dc_item_mapping im ON "dcinv"."asdcitemid" = "im"."asdcitemid"
   LEFT JOIN dsipc.dc_inventory_delete dinv ON "dcinv"."asdcinventoryid" = "dinv"."asdcinventoryid"
   WHERE dinv.asdcinventoryid IS NULL
)  i1 ON i1.product_number = b1.product_number AND i1.inventory_date = b1.last_reported AND i1.trading_partner_number = b1.trading_partner_number
LEFT JOIN dsipc.product_details p1 ON i1.product_number = p1.product_number
LEFT JOIN dsipc.trading_partner_details t1 ON i1.trading_partner_number = t1.trading_partner_number
LEFT JOIN dsipc.seasonal_factor s1 ON i1.inventory_date = s1.cal_date
CROSS JOIN (
   SELECT max(inventory_date) latest_inventory_date
   FROM
     dsipc.dc_inv_po dcinv
)  i2
LEFT JOIN (
   SELECT
     procurer_number buyer_number
   , product_number
  ,date_trunc('month', ew.start_date) reporting_month
   , currency
   , avg(delivered_price) contract_delivered_price
   FROM
     dsipc.contract_pricing_ew ew
   GROUP BY 1, 2, 3,4
)  i3 ON p1.product_number = i3.product_number AND t1.trading_partner_number = i3.buyer_number AND date_trunc('month', i1.inventory_date) = date_trunc('month', i3.reporting_month)
LEFT JOIN (
   SELECT
     procurer_number buyer_number
   , product_number
  ,date_trunc('month', ew.start_date) reporting_month
   , currency
   , avg(delivered_price) contract_delivered_price
   FROM dsipc.contract_pricing_ew ew
   GROUP BY 1, 2, 3,4
)  i4 ON p1.product_number = i4.product_number AND t1.trading_partner_number = i4.buyer_number AND date_trunc('month', i1.inventory_date) = date_trunc('month', date_add('month', -1, i4.reporting_month))
LEFT JOIN (
   SELECT
     procurer_number buyer_number
   , product_number
  ,date_trunc('month', ew.start_date) reporting_month
   , currency
   , avg(delivered_price) contract_delivered_price
   FROM dsipc.contract_pricing_ew ew
   GROUP BY 1, 2, 3,4
)  i5 ON p1.product_number = i5.product_number AND t1.trading_partner_number = i5.buyer_number AND date_trunc('month', i1.inventory_date) = date_trunc('month', date_add('month', 1, i5.reporting_month))


left join --W1 from invoices
(
SELECT
invoices.asdcitemid,
--p1.gtin,
--cm.dcextid,
date(DATEADD(d,-DATEPART(dow, invoices.invoice_date), invoices.invoice_date)) as invoices_week_start_date,
SUM(invoices.delivered_quantity) as week_1_sales
FROM dsipc.dc_invoice_transaction invoices
INNER JOIN dsipc.dc_item_mapping im ON "invoices"."asdcitemid" = "im"."asdcitemid"
INNER JOIN dsipc.dc_customer_mapping cm ON "invoices"."ascustomerid" = "cm"."ascustomerid"
LEFT JOIN dsipc.dc_invoice_delete dinv ON "invoices"."asinvoiceid" = "dinv"."asinvoiceid"
LEFT JOIN dsipc.product_details p1 ON "im"."extid" = "p1"."product_number"

--GROUP by invoices_week_start_date, p1.gtin, cm.dcextid
GROUP by invoices_week_start_date, invoices.asdcitemid
) i6
on i6.asdcitemid = i1.asdcitemid
--on i6.dcextid = t1.trading_partner_number
--and i6.gtin = p1.gtin
and i6.invoices_week_start_date = date(DATEADD(d,-DATEPART(dow, b1.inventory_date), b1.inventory_date)) - 7

  
left join --WTD from invoices
(
SELECT --DISTINCT
invoices.asdcitemid,
--p1.gtin,
--cm.dcextid,
invoices.invoice_date,
date(DATEADD(d,-DATEPART(dow, invoices.invoice_date), invoices.invoice_date)) as invoices_week_start_date,
invoices.delivered_quantity as total_wtd_sales_qty
FROM dsipc.dc_invoice_transaction invoices
INNER JOIN dsipc.dc_item_mapping im ON "invoices"."asdcitemid" = "im"."asdcitemid"
INNER JOIN dsipc.dc_customer_mapping cm ON "invoices"."ascustomerid" = "cm"."ascustomerid"
LEFT JOIN dsipc.dc_invoice_delete dinv ON "invoices"."asinvoiceid" = "dinv"."asinvoiceid"
LEFT JOIN dsipc.product_details p1 ON "im"."extid" = "p1"."product_number"

) i7
on i7.asdcitemid = i1.asdcitemid
--on i7.dcextid = t1.trading_partner_number
--and i7.gtin = p1.gtin

and i7.invoices_week_start_date = date(DATEADD(d,-DATEPART(dow, b1.inventory_date), b1.inventory_date))
and i7.invoice_date <= b1.inventory_date

GROUP by b1.inventory_date, i6.week_1_sales, b1.last_reported, i2.latest_inventory_date, t1.trading_partner_type, t1.is_active, t1.trading_partner_name, t1.shipping_city , t1.shipping_state , t1.shipping_zip,t1.shipping_longitude , t1.shipping_latitude , t1.ipc_region, t1.ipc_zone, t1.ipc_zdm, t1.ipc_rdm, t1.gln, p1.gtin, p1.product_number, p1.brand_name , p1.product_name, p1.product_group,p1.category_l1, p1.category_l2, p1.category_l3, p1.category_l4, p1.category_l5, p1.product_deleted, p1.product_country, p1.manufacturer_number, p1.manufacturer_name, i1.dc_product_number, i1.mfr_product_number, i1.number_of_stores, t1.trading_partner_number, i1.pack, i1.pack_size, i1.po1_num, i1.po1_req_date, i1.po1_dlv_date, i1.po2_num , i1.po2_req_date, i1.po2_dlv_date, i1.po3_num, i1.po3_req_date, i1.po3_dlv_date, i1.po4_num , i1.po4_req_date, i1.po4_dlv_date, i1.po5_num, i1.po5_req_date, i1.po5_dlv_date, i1.last_rcvd_date, i1.last_rcvd_trx_date, i1.inventory_status, i1.max_allowed_qty, i3.contract_delivered_price, i4.contract_delivered_price, i5.contract_delivered_price, i1.total_wtd_sales_qty, i1.total_on_hand_qty, i1.week_1_sales, i1.week_2_sales, i1.week_3_sales, i1.week_4_sales, i1.week_5_sales, i1.week_6_sales, i1.week_7_sales, i1.week_8_sales, i1.week_9_sales, i1.week_11_sales, i1.week_12_sales, i1.week_13_sales, i1.week_10_sales, i1.boh_qty, i1.commit_qty, i1.unavailable_qty, i1.boh_cs_store, i1.adjustments, i1.days_on_hand, i1.days_on_hand_on_order, i1.days_until_appt, i1.doh_days_until_appt, i1.total_on_order, i1.po1_qty, i1.po2_qty, i1.po3_qty, i1.po4_qty, i1.po5_qty, i1.total_rcvd_wtd, i1.last_rcvd_qty, s1.cad_moving_seasonal_factor, s1.moving_seasonal_factor, t1.shipping_country
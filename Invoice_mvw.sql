CREATE MATERIALIZED VIEW dsipc.ds_dc_invoices_mvw 
BACKUP NO
AUTO REFRESH YES 
AS
select 
  "r1"."restaurant_address1"
, "r1"."restaurant_address2"
, "r1"."restaurant_city"
, "r1"."restaurant_state"
, "r1"."restaurant_zip"
, "r1"."restaurant_country"
, "r1"."restaurant_state_code"
, "r1"."restaurant_type"
, "r1"."faf_market" "market_number"
, "r1"."market_region" "market_name"
, '' "market_region"
, "r1"."faf_dash_group" "market_dash_group"
, "r1"."restaurant_da_id" "restaurant_da_number"
, "r1"."restaurant_da_name"
, "r1"."restaurant_region"
, r1.restaurant_primary_dc_name
,r1.restaurant_primary_dc_id
, "t1"."trading_partner_type"
, "t1"."is_active" "dc_active"
, "t1"."trading_partner_par_name" "dc_parent_name"
, "t1"."trading_partner_name" "dc_name"
, "t1"."shipping_city" "dc_city"
, "t1"."shipping_state" "dc_state"
, "t1"."shipping_zip" "dc_zip"
, "t1"."shipping_country" "dc_country"
, "t1"."shipping_longitude" "dc_longitude"
, "t1"."shipping_latitude" "dc_latitude"
, "t1"."ipc_region"
, "t1"."ipc_zone"
, "t1"."gln"
, "p1"."gtin"
, "p1"."product_name"
,p1.net_weight_lb 
, "p1"."product_group"
, "p1"."category_l1"
, "p1"."category_l2"
, "p1"."category_l3"
, "p1"."category_l4"
, "p1"."category_l5"
, "p1"."product_deleted"
, "p1"."product_del_date"
, "p1"."inactive_date"
, "p1"."manufacturer_number"
, "p1"."manufacturer_name"
, "dcinv"."invoice_date"
, date_part(w,"dcinv"."invoice_date") "invoice_week"
--, "date_add"('day', -1, "date_trunc"('week', "dcinv"."invoice_date")) "week_start_date"
, date(DATEADD(d,-DATEPART(dow, dcinv.invoice_date), dcinv.invoice_date)) as week_start_date
, "dcinv"."invoice_number"
, NVL("dcinv"."delivered_quantity", 0.0 ) "delivered_quantity"
, NVL("dcinv"."unit_price", 0.0)  "unit_price"
, NVL("dcinv"."delivered_cost", 0.0) "delivered_cost"
,CASE WHEN ((dcinv.po_number is NULL) OR (dcinv.po_number = '')) THEN 'UNKNOWN' ELSE dcinv.po_number END po_number             
, dcinv.case_multiplier
, "dcinv"."quantityuom" "quantity_uom"
, "dcinv"."dc_product_number"
, dcinv.dc_product_name 
, "p1"."ipc_pk_qty" "pack_quantity"
, "p1"."ipc_pack_size" "pack_size"
, NVL("i3"."contract_delivered_price", 0.0) "lcr_delivered_price"
, NVL((i3.contract_delivered_price * dcinv.delivered_quantity), 0.0)  "lcr_inventory_value"
, (CASE WHEN (("cm"."extid" IS NULL) OR ("cm"."extid" = '')) THEN 'WaitingForArrowstream' ELSE "cm"."extid" END) "restaurant_number"
, (CASE WHEN (("im"."extid" IS NULL) OR ("im"."extid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."extid" END) "product_number"
, (CASE WHEN (("im"."dcextid" IS NULL) OR ("im"."dcextid" = '')) THEN 'WaitingForArrowstream' ELSE "im"."dcextid" END) "dc_number"

FROM
 dsipc.dc_invoice_transaction dcinv

INNER JOIN dsipc.dc_customer_mapping cm ON "dcinv"."ascustomerid" = "cm"."ascustomerid"
INNER JOIN dsipc.dc_item_mapping im ON "dcinv"."asdcitemid" = "im"."asdcitemid"
LEFT JOIN dsipc.dc_invoice_delete dinv ON "dcinv"."asinvoiceid" = "dinv"."asinvoiceid"
LEFT JOIN dsipc.restaurant_details r1 ON "cm"."extid" = "r1"."restaurant_number_w_sat"
LEFT JOIN dsipc.product_details p1 ON "im"."extid" = "p1"."product_number"
LEFT JOIN dsipc.trading_partner_details t1 ON "im"."dcextid" = "t1"."trading_partner_number"
LEFT JOIN (
   SELECT
     "procurer_number", "product_number", "reporting_month" , "currency", "avg"("landed_cost_total") "contract_delivered_price"
   FROM
     (SELECT DISTINCT "date_trunc"('month', "ew"."start_date") "reporting_month" , "ew"."currency" 
           , COALESCE("ew"."landed_cost_total", 0) "landed_cost_total" 
           , "p1"."product_number" , "ew"."procurer_number" 
     FROM dsipc.contract_pricing_ew ew 
     LEFT JOIN dsipc.product_details p1 ON "ew"."product_number" = "p1"."product_number"
     LEFT JOIN  dsipc.trading_partner_details t1 ON "t1"."trading_partner_number" = "ew"."seller_number"
     LEFT JOIN dsipc.trading_partner_details t2 ON "t2"."trading_partner_number" = "ew"."procurer_number"
     LEFT JOIN dsipc.trading_partner_details t3 ON "t3"."trading_partner_number" = "p1"."manufacturer_number"
     ) ew        
   GROUP BY "procurer_number", "product_number", "reporting_month", "currency"
)  i3 ON "p1"."product_number" = "i3"."product_number" AND "t1"."trading_partner_number" = "i3"."procurer_number" AND date_trunc('month',"dcinv"."invoice_date") = "reporting_month"
WHERE ("dinv"."asinvoiceid" IS NULL) ;


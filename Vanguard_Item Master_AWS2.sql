----sql to get data for ItemMaster
---------------------------------------------------

SELECT DISTINCT di.category_l4 as item,
                di.dc_number as whse,
                di.dc_name,
                di.dc_country,
                ISNULL(R1.store_count,0) as store_count,
                di.market_number,
                R2.market_name,
				di.category_l4 ||','|| di.dc_number as display_name,
                di.category_l1,
                di.category_l2,
                di.category_l3,
				di.category_l4 as product_group,
				ISNULL(sdp_final.AvgSellPrice, 0) AS AvgSellPrice,
				ISNULL(lcr_final.COGs, 0) AS COGs,
                ISNULL(pr.FOB, 0) AS FOB,
                CASE WHEN di.dc_country = 'CANADA' THEN 'CAD'
                     ELSE 'USD'
                END as unit_base_currency,
                pd.qms_type as CompartmentType
FROM
    (
    SELECT category_l1,
        category_l2,
        category_l3,
        category_l4,
        dc_number,
        dc_name,
        dc_country,
        market_number_update AS market_number
    FROM (SELECT distinct category_l1,
                          category_l2,
                          category_l3,
                            category_l4,
                            dc_number,
                            dc_name,
                            dc_country,
                            invoice_date,
                            market_number,
                            replace(market_number, right(market_number, 2), '-0') as market_number_update,
                            product_number,
                            product_name
            FROM "dsipc"."ds_dc_invoices_mvw"
            WHERE category_l2 NOT LIKE 'BTL'
            AND category_l2 NOT LIKE '%-3'
            AND category_l2 NOT LIKE '%-4'
            AND category_l2 NOT LIKE 'FWH PENDING%'
            AND market_number NOT LIKE 'NULL'
			AND invoice_date >= '2018-01-01'
            ORDER BY invoice_date, dc_number, dc_name) AS dc_temp
    GROUP BY category_l1,
             category_l2,
             category_l3,
             category_l4,
             dc_number,
             dc_name,
             dc_country,
             market_number_update
    ) di



LEFT JOIN --this join gets the FOB
    (
	
	select 
        cp.buyer_number,
        cp.buyer_name,
		cp.category_l4,
        avg(cp.delivered_price) as FOB
    from "dsipc"."contract_pricing_ew_mvw" cp
    
    
    inner join (
    select buyer_number,
    category_l4,
    max(pricing_start_date) as MaxDate
    from "dsipc"."contract_pricing_ew_mvw"
    where pricing_start_date >= '2018-01-01'
    group by buyer_number, category_l4
    ) as fob_table
	ON cp.category_l4 = fob_table.category_l4 and cp.pricing_start_date = fob_table.MaxDate and cp.buyer_number = fob_table.buyer_number
    
    where cp.seller_number NOT LIKE '%RE%'
      
    group by cp.buyer_number, cp.buyer_name, cp.category_l4 e
    order by cp.buyer_number, cp.buyer_name, cp.category_l4 
	
	) pr
ON di.dc_number = pr.buyer_number
AND di.category_l4 = pr.category_l4



LEFT JOIN --this join gets the DC delivered price = COGs
    (
	
	select lcr.dc_number,
        lcr.dc_name,
 		lcr.category_l4,
        avg(lcr.lcr_delivered_price) as COGs
    from "dsipc"."ds_dc_invoices_mvw" lcr
    
    
    inner join (
    select dc_number,
    category_l4,
    max(invoice_date) as MaxDate
    from "dsipc"."ds_dc_invoices_mvw"
    where invoice_date >= '2018-01-01'
    group by dc_number, category_l4
    ) as date_table
	
	ON lcr.category_l4 = date_table.category_l4 and lcr.invoice_date = date_table.MaxDate and lcr.dc_number = date_table.dc_number
    
    where lcr.dc_number NOT LIKE '%RE%' 
	
    group by lcr.dc_number, lcr.dc_name, lcr.category_l4
    order by lcr.dc_number, lcr.dc_name, lcr.category_l4
	
	) lcr_final
ON di.dc_number = lcr_final.dc_number
AND di.category_l4 = lcr_final.category_l4


LEFT JOIN --this join gets the product temperature
    (select distinct 
		category_l4,
		isnull((CASE WHEN qms_type ='%%' THEN 'NA' END),'NA') as qms_type
    from "dsipc"."product_details"
    ) pd
ON di.product_number = pd.product_number
ON di.category_l4 = pd.category_l4



LEFT JOIN --this join gets the store delivered price = avgsellprice
    (
	
	select sdp.dc_number,
        sdp.dc_name,
 		sdp.category_l4,
        (CASE WHEN SUM(delivered_quantity) = 0 THEN 0
             WHEN SUM(delivered_quantity) != 0 THEN sum(delivered_cost)/sum(delivered_quantity)
        END) as avgsellprice
		
    from "dsipc"."ds_dc_invoices_mvw" sdp
    
    
    inner join (
    select dc_number,
    category_l4,
    max(invoice_date) as MaxDate
    from "dsipc"."ds_dc_invoices_mvw"
    where invoice_date >= '2018-01-01'
    group by dc_number, category_l4
    ) as date_table
	
	ON sdp.category_l4 = date_table.category_l4 and sdp.invoice_date = date_table.MaxDate and sdp.dc_number = date_table.dc_number
    
    where sdp.dc_number NOT LIKE '%RE%'
	
    group by sdp.dc_number, sdp.dc_name, sdp.category_l4
    order by sdp.dc_number, sdp.dc_name, sdp.category_l4
	
	) sdp_final
ON di.dc_number = sdp_final.dc_number
AND di.category_l4 = sdp_final.category_l4


LEFT JOIN --this join gets the store count for each DC and market ending in -0
  (select restaurant_primary_dc_id as dc_number,
		SUBSTRING(faf_market, 3) as mkt_no_dash,
        count(restaurant_number_w_sat) as store_count
        from "dsipc"."restaurant_details"
        where restaurant_status like '%Open%'
        and restaurant_country in ('UNITED STATES', 'CANADA')
   group by restaurant_primary_dc_id, mkt_no_dash
   order by restaurant_primary_dc_id, mkt_no_dash
   ) R1
ON di.dc_number = R1.dc_number
AND di.market_number = CONCAT(R1.mkt_no_dash,'-0')



LEFT JOIN --this join gets the market names
    (SELECT distinct faf_market as market_number, market_region as market_name
     from "dsipc"."restaurant_details"
     where restaurant_country in ('UNITED STATES', 'CANADA')
     and faf_market like '%-0%'
     and restaurant_status = 'Open and Operating'
     and (restaurant_primary_dc_id not like 'NULL' or restaurant_primary_dc_id <> '')
    ) R2
ON di.market_number = R2.market_number
ON replace(tb.faf_market, right(tb.faf_market, 2), '-0') = R2.market_number


--where di.category_l4 = 'PORK/ HAM' --for testing
--AND di.dc_number = 'DIST250'
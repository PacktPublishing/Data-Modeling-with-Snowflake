/*
 * 
 * 
 * 2. Recovering physically deleted records
 * 	  and performing a clean "logical" delete
 * 
 * 
 */





-- reset the src and rb tables back to baseline after the first exercise
CREATE OR REPLACE TABLE src_lineitem CLONE src_lineitem_bak;
CREATE OR REPLACE TABLE lineitem_rb CLONE lineitem_rb_bak;
CREATE OR replace STREAM strm_lineitem_rb ON TABLE lineitem_rb;


---------------------------------------------------------------------------------------------------------------------
-- Perform simulated daily load
---------------------------------------------------------------------------------------------------------------------

--load src table eliminating some records as deletions
--line numbers ending in 0,1, and 2 will be filtered out 
--to simulate deletions.
INSERT INTO src_lineitem
WITH updates AS (
SELECT DISTINCT sales_order_id FROM src_lineitem SAMPLE (10 rows)
)
--updates
SELECT 
	line_number
	, src.sales_order_id
	, part_id
	, supplier_id
	, quantity + supplier_id
	, extended_price_usd + 1000
	, discount_percent + .01 
	, tax_percent
	, CASE WHEN return_flag = 'N' AND right(part_id,1) > 4 THEN 'R' ELSE return_flag END
	, line_status 
	, ship_date+1
	, commit_date+2
	, receipt_date+3
	, ship_instructions
	, ship_mode
	, COMMENT
	, current_timestamp()
	, 'update'
FROM source_system_lineitem src 
INNER JOIN updates up ON src.sales_order_id = up.sales_order_id
WHERE TRUE 
AND right(line_number,1)::int > 2 --simulate deleted records
;

---------------------------------------------------------------------------------------------------------------------
-- Recover deleted records
---------------------------------------------------------------------------------------------------------------------


--get a list of all load dates for the orders loaded (today or all time)
CREATE OR REPLACE TEMPORARY TABLE line_load_hist
AS
  (
   select *
   from (SELECT DISTINCT
        __ldts      ,
        sales_order_id     ,
        --calculate next load date. This will be null in the latest load only
        lead(__ldts) over ( PARTITION BY sales_order_id ORDER BY __ldts) next_order_load_dt 
	        FROM
	        	(
	            SELECT  DISTINCT __ldts, sales_order_id
	        	FROM src_lineitem
	                WHERE TRUE 
	        	    AND __load_type != 'deletion'
	        	 ) 
    	 )  WHERE TRUE 
        --select only records from the latest (daily) load
        --comment out the next line for a full reload
    		AND next_order_load_dt = (SELECT MAX(__ldts) FROM src_lineitem)
);


CREATE OR REPLACE TEMPORARY TABLE line_deletions AS 
SELECT a.next_order_load_dt, b.*
FROM
	(
	SELECT
	--lead load date to determine if it has been deleted
	--null values mean it has been deleted (or latest)
      lead(__ldts) over(partition by sales_order_id, line_number  order by __ldts asc) AS  next_line_load_dt
	, __ldts
    , sales_order_id
	, line_number 
	, __load_type
	FROM	   src_lineitem
	WHERE TRUE
    --AND __load_type != 'deletion'
 				) b
INNER JOIN line_load_hist a
ON		(a.sales_order_id = b.sales_order_id
AND     a.__ldts = b.__ldts)                            
WHERE TRUE 
    AND a.next_order_load_dt IS NOT NULL --only true for latest record
/* uncomment the two lines below if deleted lineitem numbers can be reused
 * in this example, they can not 
*/ 
    AND --(
    	b.next_line_load_dt IS NULL 
    		--OR a.next_order_load_dt != nvl(b.next_line_load_dt,current_timestamp))
;


--insert the recovered record into the source table
--making sure to zero out all additive measures
--also, the load date must be the latest, not the original
INSERT INTO src_lineitem
(
	line_number
	, sales_order_id
	, part_id
	, supplier_id
	, quantity
	, extended_price_usd
	, discount_percent
	, tax_percent
	, return_flag
	, line_status
	, ship_date
	, commit_date
	, receipt_date
	, ship_instructions
	, ship_mode
	, comment
	, __ldts
	, __load_type
)
WITH deleted AS (
SELECT del.next_order_load_dt, tgt.* FROM src_lineitem tgt
INNER JOIN line_deletions del
USING (sales_order_id, line_number, __ldts)
WHERE TRUE  
AND del.__load_type != 'deletion'
 )
--"logically" delete the record by inserting it with 0-value measures
--remember to treat non-additive measures as attributes  
SELECT 
	line_number
	, sales_order_id
	, part_id
	, supplier_id
	, 0 --quantity
	, 0 --extended_price_usd
	, discount_percent
	, tax_percent
	, return_flag
	, line_status
	, ship_date
	, commit_date
	, receipt_date
	, ship_instructions
	, ship_mode
	, comment
	, next_order_load_dt  --the load date when the deletion happened 
	, 'deletion' 
FROM deleted
WHERE TRUE                  
; 
 


---------------------------------------------------------------------------------------------------------------------
-- Observe the impact on the fact table
---------------------------------------------------------------------------------------------------------------------

--these are the same steps from the first exercise to load the reverse balance table

--load the after-image to the reverse balance table
MERGE INTO lineitem_rb rb
--get only latest records from src_lineitem.
USING (SELECT srcl.*, max_asat  FROM src_lineitem  srcl
--we only want to update the latest record for each order, 
--so we need to know the asat date for every sales_order_id and its corresponding line items		
		LEFT JOIN 	(SELECT 
				 sales_order_id, max(asat_dts) max_asat FROM lineitem_rb GROUP BY 1
				) latest_asat 
		ON srcl.sales_order_id = latest_asat.sales_order_id
		WHERE __ldts = (SELECT MAX(__ldts) FROM src_lineitem) 
	) src
ON  rb.line_number = 	src.line_number
AND rb.sales_order_id = src.sales_order_id
AND rb.asat_dts = src.max_asat
AND rb.is_afterimage
WHEN NOT MATCHED 
--new records, insert
THEN INSERT VALUES (
	 __load_type --(insert)
	,__ldts
	, TRUE
	, line_number
	, sales_order_id
	, part_id
	, supplier_id
	, quantity
	, extended_price_usd
	, discount_percent
	, tax_percent
	, return_flag
	, line_status
	, ship_date
	, commit_date
	, receipt_date
	, ship_instructions
	, ship_mode
	, COMMENT
)
WHEN MATCHED --record EXISTS, therefore is UPDATE 
THEN UPDATE 
SET  
	--update only the fields that can change
  	 rb.__load_type = src.__load_type --(update)
	,rb.asat_dts = src.__ldts
	--afterimage is already = true
	,rb.quantity = src.quantity
	,rb.extended_price_usd = src.extended_price_usd
	,rb.discount_percent = src.discount_percent
	,rb.return_flag = src.return_flag
	,rb.ship_date = src.ship_date
	,rb.commit_date = src.commit_date
	,rb.receipt_date = src.receipt_date
;




	  
--insert the before image from the stream
INSERT INTO lineitem_rb
WITH before_records AS (
--get the before image but append the asat from the after record 
	SELECT bf.*, asat_after FROM strm_lineitem_rb bf                                                   --before
	INNER JOIN (SELECT line_number, sales_order_id, asat_dts AS asat_after  FROM strm_lineitem_rb	 --AFTER 
				WHERE metadata$action = 'INSERT'
				AND metadata$isupdate 
				) af 			
	USING (line_number, sales_order_id)			
WHERE TRUE 
AND bf.metadata$action = 'DELETE'  
)
--insert the original after image that we updated
--no changes are required to column values
SELECT
	 __load_type 
	, asat_dts
	, is_afterimage --(true)
	, line_number
	, sales_order_id
	, part_id
	, supplier_id
	, quantity
	, extended_price_usd
	, discount_percent
	, tax_percent
	, return_flag
	, line_status
	, ship_date
	, commit_date
	, receipt_date
	, ship_instructions
	, ship_mode
	, COMMENT
FROM 	before_records
UNION ALL
--insert the before image as at after dts, but negate additive measures
SELECT 
	 __load_type 
	, asat_after --USE the asat OF the AFTER image
	, FALSE  --FALSE because this IS the BEFORE image
	, line_number
	, sales_order_id
	, part_id
	, supplier_id
	, -1 * quantity 
	, -1 * extended_price_usd
	, discount_percent --do NOT negate because this IS non-additive 
	, tax_percent
	, return_flag
	, line_status
	, ship_date
	, commit_date
	, receipt_date
	, ship_instructions
	, ship_mode
	, COMMENT
FROM 	before_records
;



---------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------

 

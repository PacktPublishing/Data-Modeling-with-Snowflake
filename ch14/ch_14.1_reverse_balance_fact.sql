/*
 * 
 * 14.1 Reverse balance fact table
 * 
 * 
 */
 
 
--------------------------------------------------------------------
-- setting up the environment
--------------------------------------------------------------------
CREATE OR REPLACE SCHEMA ch14_facts;



---------------------------------------------------------------------------------------------------------------------
-- Prepare the base tables
---------------------------------------------------------------------------------------------------------------------


--create "source system" sample data
CREATE OR REPLACE TABLE source_system_lineitem
(
 line_number        number(38,0) NOT NULL,
 sales_order_id     number(38,0) NOT NULL,
 part_id            number(38,0) NOT NULL,
 supplier_id        number(38,0) NOT NULL,
 quantity           number(12,2),
 extended_price_usd number(12,2),
 discount_percent   number(12,2),
 tax_percent        number(12,2),
 return_flag        varchar(1),
 line_status        varchar(1),
 ship_date          date,
 commit_date        date,
 receipt_date       date,
 ship_instructions  varchar(25),
 ship_mode          varchar(10),
 comment            varchar(44),
 CONSTRAINT pk_lineitem PRIMARY KEY ( line_number, sales_order_id ) 
)
COMMENT = 'various line items per order'
AS 
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment 
FROM snowflake_sample_data.tpch_sf10.lineitem  
;


--create DWH landing area
CREATE OR REPLACE TABLE src_lineitem
(
 line_number        number(38,0) NOT NULL,
 sales_order_id     number(38,0) NOT NULL,
 part_id            number(38,0) NOT NULL,
 supplier_id        number(38,0) NOT NULL,
 quantity           number(12,2),
 extended_price_usd number(12,2),
 discount_percent   number(12,2),
 tax_percent        number(12,2),
 return_flag        varchar(1),
 line_status        varchar(1),
 ship_date          date,
 commit_date        date,
 receipt_date       date,
 ship_instructions  varchar(25),
 ship_mode          varchar(10),
 comment            varchar(44),
 __ldts				TIMESTAMP_NTZ,
 __load_type			varchar, --FOR testing 
 CONSTRAINT pk_lineitem PRIMARY KEY ( line_number, sales_order_id, __ldts ) 
)
COMMENT = 'various line items per order'
AS 
WITH complete_orders AS (
SELECT DISTINCT sales_order_id FROM source_system_lineitem SAMPLE (10000 rows) 
) 
SELECT
	line_number
	, src.sales_order_id
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
	, current_timestamp()
	, 'initial'
FROM source_system_lineitem src 
INNER JOIN complete_orders co ON src.sales_order_id = co.sales_order_id
;

-- create a backup of src_lineitem for re-running the example
CREATE OR REPLACE TABLE src_lineitem_bak CLONE src_lineitem;


-- create the initial load of the reverse balance table (no changes yet)
CREATE OR replace TABLE lineitem_rb
(
 __load_type		varchar, --will make it easier to understand the records in the example
 asat_dts			TIMESTAMP_NTZ 	 NOT NULL, --in a daily load, this should be a date type 
 is_afterimage		boolean		 NOT NULL,
 line_number        number(38,0) NOT NULL,
 sales_order_id     number(38,0) NOT NULL,
 part_id            number(38,0) NOT NULL,
 supplier_id        number(38,0) NOT NULL,
 quantity           number(12,2),
 extended_price_usd number(12,2),
 discount_percent   number(12,2),
 tax_percent        number(12,2),
 return_flag        varchar(1),
 line_status        varchar(1),
 ship_date          date,
 commit_date        date,
 receipt_date       date,
 ship_instructions  varchar(25),
 ship_mode          varchar(10),
 comment            varchar(44),
 /*
  * diff_hash		varchar(32) --this example does not use a diff_hash because 
  * 							--matching incoming records are understood to be updates.
  * 							--Otherwise, use a diff_hash for easy compare.
  */
 CONSTRAINT pk_lineitem PRIMARY KEY ( line_number, sales_order_id, asat_dts, is_afterimage ) 
)
COMMENT = 'reverse balance for changes in line items'
AS 
SELECT
	 __load_type
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
	, comment
FROM
	src_lineitem
;

--create a backup for re-running loads
CREATE OR REPLACE TABLE lineitem_rb_bak CLONE lineitem_rb;

--create a stream for future updates
CREATE OR replace STREAM strm_lineitem_rb ON TABLE lineitem_rb;


---------------------------------------------------------------------------------------------------------------------
-- Perform daily loads
---------------------------------------------------------------------------------------------------------------------

--for re-running the exercise 
--CREATE OR REPLACE TABLE src_lineitem CLONE src_lineitem_bak;
--CREATE OR REPLACE TABLE lineitem_rb CLONE lineitem_rb_bak;

--load src table with 10 new and 10 updated (whole) orders
INSERT INTO src_lineitem
WITH new_not_existing AS (
SELECT DISTINCT sales_order_id FROM source_system_lineitem SAMPLE (10 rows)
EXCEPT 
SELECT DISTINCT sales_order_id FROM src_lineitem 
)
, updates AS (
SELECT DISTINCT sales_order_id FROM src_lineitem SAMPLE (10 rows)
)
--new records
SELECT 
	line_number
	, src.sales_order_id
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
	, current_timestamp()
	, 'initial'
FROM source_system_lineitem src 
INNER JOIN new_not_existing ne ON src.sales_order_id = ne.sales_order_id
UNION ALL 
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
;


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

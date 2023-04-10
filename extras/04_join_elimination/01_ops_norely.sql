create or replace schema ops_norely;


-- ************************************** part
CREATE  TABLE part
(
 part_id          number(38,0) NOT NULL,
 name             varchar NOT NULL,
 manufacturer     varchar NOT NULL,
 brand            varchar NOT NULL,
 type             varchar NOT NULL,
 size_centimeters number(38,0) NOT NULL,
 container        varchar NOT NULL,
 retail_price_usd number(12,2) NOT NULL,
 comment          varchar COMMENT 'varchar COMMENT ''VARCHAR(23)',

 CONSTRAINT pk_part PRIMARY KEY ( part_id ) 
)
COMMENT = 'Parts we distribute'
AS 
SELECT p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment  
FROM snowflake_sample_data.tpch_sf10.part  
;

-- ************************************** location
CREATE  TABLE location
(
 location_id number(38,0) NOT NULL,
 name        varchar(25) NOT NULL,
 region_id   number(38,0) NOT NULL,
 comment     varchar(152) COMMENT 'varchar(152) COMMENT ''VARCHAR(152)',
 CONSTRAINT pk_location PRIMARY KEY ( location_id) ,
 CONSTRAINT ak_location_name UNIQUE ( name ) 
)
COMMENT = 'location assigned to 
customer or supplier'
AS 
SELECT n_nationkey, n_name, n_regionkey, n_comment  
FROM snowflake_sample_data.tpch_sf10.nation  
;

-- ************************************** supplier
CREATE  TABLE supplier
(
 supplier_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 comment             varchar COMMENT 'varchar COMMENT ''VARCHAR(101)',
 CONSTRAINT pk_supplier PRIMARY KEY ( supplier_id ) ,
 CONSTRAINT FK_SUPPLIER_BASED_IN_LOCATION FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) 
)
COMMENT = 'Suppliers who we buy from'
AS 
SELECT s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment  
FROM snowflake_sample_data.tpch_sf10.supplier
;

-- ************************************** customer
CREATE  TABLE customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'varchar COMMENT ''VARCHAR(117)',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id ) ,
 CONSTRAINT FK_CUSTOMER_BASED_IN_LOCATION FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) 
)
COMMENT = 'Registered cusotmers'
AS 
SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment  
FROM snowflake_sample_data.tpch_sf10.customer
;

-- ************************************** sales_order
CREATE  TABLE sales_order
(
 sales_order_id  number(38,0) NOT NULL,
 customer_id     number(38,0) NOT NULL,
 order_status    varchar(1),
 total_price_usd number(12,2),
 order_date      date,
 order_priority  varchar(15),
 clerk           varchar(15),
 ship_priority   number(38,0),
 comment         varchar(79) COMMENT 'varchar(79) COMMENT ''VARCHAR(79)',

 CONSTRAINT pk_sales_order PRIMARY KEY ( sales_order_id ) ,
 CONSTRAINT FK_SALES_ORDER_PLACED_BY_CUSTOMER FOREIGN KEY ( customer_id ) REFERENCES customer ( customer_id ) 
)
COMMENT = 'single order per customer'
AS 
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment  
FROM snowflake_sample_data.tpch_sf10.orders 
;


-- ************************************** inventory
CREATE  TABLE inventory
(
 part_id           number(38,0) NOT NULL COMMENT 'part of unique identifier with ps_suppkey',
 supplier_id       number(38,0) NOT NULL COMMENT 'part of unique identifier with ps_partkey',
 available_amount  number(38,0) NOT NULL COMMENT 'number of parts available for sale',
 supplier_cost_usd number(12,2) NOT NULL COMMENT 'original cost paid to supplier',
 comment           varchar() COMMENT 'varchar(79) COMMENT ''VARCHAR(79)',

 CONSTRAINT pk_inventory PRIMARY KEY ( part_id, supplier_id ) ,
 CONSTRAINT FK_INVENTORY_STORES_PART FOREIGN KEY ( part_id ) REFERENCES part ( part_id ) ,
 CONSTRAINT FK_INVENTORY_SUPPLIED_BY_SUPPLIER FOREIGN KEY ( supplier_id ) REFERENCES supplier ( supplier_id ) 
)
COMMENT = 'Warehouse Inventory'
AS 
SELECT ps_partkey, ps_suppkey, ps_availqty, ps_supplycost, ps_comment
FROM snowflake_sample_data.tpch_sf10.partsupp  
;

-- ************************************** lineitem
CREATE  TABLE lineitem
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
 comment            varchar(44) COMMENT 'varchar(44) COMMENT ''VARCHAR(44)',

 CONSTRAINT pk_lineitem PRIMARY KEY ( line_number, sales_order_id ) ,
 CONSTRAINT FK_LINEITEM_CONSISTS_OF_SALES_ORDER FOREIGN KEY ( sales_order_id ) REFERENCES sales_order ( sales_order_id ) ,
 CONSTRAINT FK_LINEITEM_CONTAINING_PART FOREIGN KEY ( part_id ) REFERENCES part ( part_id ) ,
 CONSTRAINT FK_LINEITEM_SUPPLIED_BY_SUPPLIER FOREIGN KEY ( supplier_id ) REFERENCES supplier ( supplier_id ) 
)
COMMENT = 'various line items per order'
AS 
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment 
FROM snowflake_sample_data.tpch_sf10.lineitem  
;


-- ************************************** loyalty_customer
CREATE OR REPLACE TABLE loyalty_customer
(
 customer_id   number(38,0) NOT NULL,
 level         varchar NOT NULL COMMENT 'customer full name',
 type          varchar NOT NULL COMMENT 'loyalty tier: bronze, silver, or gold',
 points_amount number NOT NULL,
 comment       varchar COMMENT 'customer loyalty status calculated from sales order volume',

 CONSTRAINT pk_loyalty_customer PRIMARY KEY ( customer_id ) ,
 CONSTRAINT fk_loyalty_customer FOREIGN KEY ( customer_id ) REFERENCES customer ( customer_id ) 
)
COMMENT = 'client loyalty program with gold, silver, bronze status'
AS 

WITH cust AS (

SELECT
        customer_id,
        name,
        address,
        location_id,
        phone,
        account_balance_usd,
        market_segment,
        comment
    FROM
        customer

)

, ord AS (

    SELECT
        sales_order_id,
        customer_id,
        order_status,
        total_price_usd,
        order_date,
        order_priority,
        clerk,
        ship_priority,
        comment
    FROM
        sales_order 
)

, cust_ord as ( 

    SELECT customer_id, sum(total_price_usd) as total_price_usd FROM (
            SELECT  o.customer_id, o.total_price_usd
            FROM ord o 
            INNER JOIN cust c 
            ON o.customer_id = c.customer_id
            WHERE TRUE
            	AND account_balance_usd > 0 --no deadbeats 
            	AND  location_id != 22 -- Excluding Russia from loyalty program will send strong message to Putin
    )
    GROUP BY customer_id
)

, business_logic AS (
    SELECT *

	    , DENSE_RANK() OVER  ( ORDER BY total_price_usd DESC ) AS  cust_level
	  
	    , CASE 
	        WHEN   cust_level BETWEEN 1 AND 20 THEN 'Gold'
	        WHEN   cust_level BETWEEN 21 AND 100 THEN 'Silver'
	        WHEN   cust_level BETWEEN 101 AND 400 THEN 'Bronze'
	               END AS loyalty_level

    FROM cust_ord
    WHERE TRUE 
    QUALIFY cust_level <= 400
    ORDER BY cust_level ASC
)  

, early_supporters as (

-- the first five customers who believed in us
    SELECT $1 AS customer_id FROM VALUES (349642), (896215) , (350965) , (404707), (509986)
)

, all_loyalty AS (

	SELECT
		 customer_id
		, loyalty_level
		, 'top 400' as type
	FROM business_logic 
	
	UNION ALL
	
	select 
		 customer_id
		, 'Gold' AS loyalty_level
		, 'early supporter' AS type
	FROM early_supporters
)

, rename AS (

	SELECT 
	  customer_id 
	, loyalty_level AS level 
	, type
    , 0 AS points_amount --will be updated by marketing team
	, '' AS comments
	 FROM all_loyalty

)

SELECT * 
FROM rename 
WHERE true
;




CREATE OR REPLACE VIEW main_reporting AS 

SELECT
	l.LINE_NUMBER
	, l.SALES_ORDER_ID
	, l.PART_ID
	, l.SUPPLIER_ID
	, l.QUANTITY
	, l.EXTENDED_PRICE_USD
	, l.DISCOUNT_PERCENT
	, l.TAX_PERCENT
	, l.RETURN_FLAG
	, l.LINE_STATUS
	, l.SHIP_DATE
	, l.COMMIT_DATE
	, l.RECEIPT_DATE
	, l.SHIP_INSTRUCTIONS
	, l.SHIP_MODE
	, l.COMMENT lineitem_comment
	, supp.NAME  AS supplier_name
	, supp.ADDRESS  AS supplier_address
	, supp.LOCATION_ID AS supplier_location_id
	, supp.PHONE AS supplier_phone
	, supp.COMMENT AS supplier_comment
    , loc_sup.NAME supplier_location_name
    , loc_sup.region_id supplier_region_id
	, sales.* EXCLUDE (sales_order_id)--,  customer_account_balance_usd, total_price_usd)
    , c.* 
     EXCLUDE customer_id 
     RENAME (ACCOUNT_BALANCE_USD as CUSTOMER_ACCOUNT_BALANCE_USD ,
             NAME as customer_name,
             COMMENT as customer_comment)
    , loc_c.NAME customer_location_name 
    , loc_c.region_id customer_region_id
	, part.*  EXCLUDE (PART_ID, retail_price_usd)
              RENAME (NAME as PART_NAME, COMMENT AS PART_COMMENT)
    , iff(loy.customer_id IS null, false,true) AS  is_loyalty_customer
    , loy.LEVEL AS loyalty_level
    , loy.TYPE AS loyalty_type
  /*, more business logic
    , more calculated fields
  */  
FROM LINEITEM l
LEFT JOIN supplier supp  on supp.supplier_id = l.supplier_id
LEFT JOIN sales_order sales on sales.sales_order_id = l.sales_order_id 
LEFT JOIN part on part.part_id = l.part_id
LEFT JOIN LOCATION loc_sup   on supp.location_id = loc_sup.location_id
LEFT JOIN customer c on sales.customer_id = c.customer_id
LEFT JOIN LOCATION loc_c ON loc_c.location_id = c.location_id
LEFT JOIN LOYALTY_CUSTOMER   loy on loy.customer_id = c.customer_id
 /*, more business dimensions
   , more business mappings
  */ 
WHERE TRUE 
;



/*
* switch between rely and norely schemas 
* to test perfrmance
use schema ops_norely;
use schema ops_rely;
*
*/


-- demonstrate join elimination in action (in OPS_RELY scheam)
SELECT current_date, ship_mode, sum(quantity) quantity FROM main_reporting
WHERE TRUE 
group by  1,2;





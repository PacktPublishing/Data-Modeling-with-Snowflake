create database demo_tpch; 

create schema operations 
WITH MANAGED ACCESS  
DATA_RETENTION_TIME_IN_DAYS = 14;



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

 CONSTRAINT pk_part PRIMARY KEY ( part_id ) RELY
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
 CONSTRAINT pk_location PRIMARY KEY ( location_id) RELY,
 CONSTRAINT ak_location_name UNIQUE ( name ) RELY
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
 CONSTRAINT pk_supplier PRIMARY KEY ( supplier_id ) RELY,
 CONSTRAINT FK_SUPPLIER_BASED_IN_LOCATION FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) RELY
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

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id ) RELY,
 CONSTRAINT FK_CUSTOMER_BASED_IN_LOCATION FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) RELY
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

 CONSTRAINT pk_sales_order PRIMARY KEY ( sales_order_id ) RELY,
 CONSTRAINT FK_SALES_ORDER_PLACED_BY_CUSTOMER FOREIGN KEY ( customer_id ) REFERENCES customer ( customer_id ) RELY
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

 CONSTRAINT pk_inventory PRIMARY KEY ( part_id, supplier_id ) RELY,
 CONSTRAINT FK_INVENTORY_STORES_PART FOREIGN KEY ( part_id ) REFERENCES part ( part_id ) RELY,
 CONSTRAINT FK_INVENTORY_SUPPLIED_BY_SUPPLIER FOREIGN KEY ( supplier_id ) REFERENCES supplier ( supplier_id ) RELY
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

 CONSTRAINT pk_lineitem PRIMARY KEY ( line_number, sales_order_id ) RELY,
 CONSTRAINT FK_LINEITEM_CONSISTS_OF_SALES_ORDER FOREIGN KEY ( sales_order_id ) REFERENCES sales_order ( sales_order_id ) RELY,
 CONSTRAINT FK_LINEITEM_CONTAINING_PART FOREIGN KEY ( part_id ) REFERENCES part ( part_id ) RELY,
 CONSTRAINT FK_LINEITEM_SUPPLIED_BY_SUPPLIER FOREIGN KEY ( supplier_id ) REFERENCES supplier ( supplier_id ) RELY
)
COMMENT = 'various line items per order'
AS 
SELECT l_orderkey, l_partkey, l_suppkey, l_linenumber, l_quantity, l_extendedprice, l_discount, l_tax, l_returnflag, l_linestatus, l_shipdate, l_commitdate, l_receiptdate, l_shipinstruct, l_shipmode, l_comment 
FROM snowflake_sample_data.tpch_sf10.lineitem  
;


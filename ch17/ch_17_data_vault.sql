/*
 * 
 * Demystifying Data Vault
 * 
 * 
 */
 
--------------------------------------------------------------------
-- setting up the warehouse
--------------------------------------------------------------------
CREATE WAREHOUSE IF NOT EXISTS demo_wh WAREHOUSE_SIZE = XSMALL;
/* or replace 'demo_wh' in the script below 
   with the name of an existing warehouse that 
   you have access to. */
 
--------------------------------------------------------------------
-- setting up environments
--------------------------------------------------------------------
CREATE OR REPLACE DATABASE data_vault;

CREATE OR REPLACE SCHEMA L0_src COMMENT = 'Schema for landing area objects';

CREATE OR REPLACE SCHEMA L1_rdv COMMENT = 'Schema for Raw Vault objects';


--------------------------------------------------------------------
-- set up the landing area
--------------------------------------------------------------------

USE SCHEMA L0_src;

/*
this exercise will pull from a single source system
called 'sys 1'.    
*/

CREATE OR REPLACE TABLE src_nation
(
 iso2_code	 varchar (2)  NOT NULL,
 n_nationkey number(38,0) NOT NULL,
 n_name      varchar(25)  NOT NULL,
 n_regionkey number(38,0) NOT NULL,
 n_comment   varchar(152) ,
 load_dts    timestamp_ntz NOT NULL,
 rec_src     varchar NOT NULL,
 
 CONSTRAINT pk_src_nation PRIMARY KEY ( n_nationkey ),
 CONSTRAINT ak_src_nation_n_name UNIQUE ( n_name ),
 CONSTRAINT ak_src_nation_iso2_code UNIQUE ( iso2_code )
 )
 COMMENT = 'ISO 3166 2-letter country codes'
AS 
SELECT *
     , CURRENT_TIMESTAMP()        
     , 'sys 1'    
FROM (     
		SELECT v.code, n.*  
		FROM snowflake_sample_data.tpch_sf10.nation n
		INNER JOIN (
		SELECT $1 id, $2 code FROM VALUES  
		(0, 'AL'),
		(1, 'AR'),
		(2, 'BR'),
		(3, 'CA'),
		(4, 'EG'),
		(5, 'ET'),
		(6, 'FR'),
		(7, 'DE'),
		(8, 'IN'),
		(9, 'ID'),
		(10, 'IR'),
		(11, 'IQ'),
		(12, 'JP'),
		(13, 'JO'),
		(14, 'KE'),
		(15, 'MA'),
		(16, 'MZ'),
		(17, 'PE'),
		(18, 'CN'),
		(19, 'RO'),
		(20, 'SA'),
		(21, 'VN'),
		(22, 'RU'),
		(23, 'GB'),
		(24, 'US')
		) v
		ON n.n_nationkey = v.id
)
;




  
CREATE OR REPLACE TABLE src_customer
(
 c_custkey    number(38,0) NOT NULL,
 c_name       varchar(25),
 c_address    varchar(40),
 iso2_code	  varchar (2)  NOT NULL,
 c_phone      varchar(15),
 c_acctbal    number(12,2),
 c_mktsegment varchar(10),
 c_comment    varchar ,
 load_dts     timestamp_ntz NOT NULL,
 rec_src      varchar NOT NULL,

 CONSTRAINT pk_src_customer PRIMARY KEY ( c_custkey )
)
COMMENT = 'registered customers with 
or without previous orders from src system 1'
;



CREATE OR REPLACE TABLE src_orders
(
 o_orderkey      number(38,0) NOT NULL,
 o_custkey       number(38,0) NOT NULL,
 o_orderstatus   varchar(1),
 o_totalprice    number(12,2),
 o_orderdate     date,
 o_orderpriority varchar(15),
 o_clerk         varchar(15),
 o_shippriority  number(38,0),
 o_comment       varchar ,
 load_dts        timestamp_ntz NOT NULL,
 rec_src         varchar NOT NULL,

 CONSTRAINT pk_src_orders PRIMARY KEY ( o_orderkey )
)
 COMMENT = 'customer order headers'
;


--------------------------------------------------------------------
-- simulate data loads from source system
--------------------------------------------------------------------

-- create streams for outbound loads to the raw vault
CREATE OR REPLACE STREAM src_customer_strm ON TABLE src_customer;
CREATE OR REPLACE STREAM src_orders_strm ON TABLE src_orders;



-- task to  simulate a subset of daily records
CREATE OR REPLACE TASK load_daily_init
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
AS 
CREATE OR REPLACE transient TABLE current_load AS 
SELECT DISTINCT c_custkey AS custkey FROM snowflake_sample_data.tpch_sf10.customer SAMPLE (1000 rows)
;



CREATE OR REPLACE TASK load_src_customer
WAREHOUSE = demo_wh
AFTER  load_daily_init
AS
INSERT INTO src_customer (
SELECT
	c_custkey
	, c_name
	, c_address
	, iso2_code
	, c_phone
	, c_acctbal
	, c_mktsegment
	, c_comment
	, current_timestamp()
	, 'sys 1'
FROM snowflake_sample_data.tpch_sf10.customer c 
INNER JOIN current_load l  ON c.c_custkey = l.custkey
INNER JOIN src_nation n ON c.c_nationkey = n.n_nationkey 
)
;






CREATE OR REPLACE TASK load_src_orders
WAREHOUSE = demo_wh
AFTER  load_daily_init
AS
INSERT INTO src_orders (
SELECT 
	o_orderkey
	, o_custkey
	, o_orderstatus
	, o_totalprice
	, o_orderdate
	, o_orderpriority
	, o_clerk
	, o_shippriority
	, o_comment
	, current_timestamp()
	, 'sys 1'
FROM snowflake_sample_data.tpch_sf10.orders o
INNER JOIN current_load l ON o.o_custkey = l.custkey
)
;

ALTER TASK load_src_customer RESUME;
ALTER TASK load_src_orders RESUME;

execute task load_daily_init;

-- save a trip to the task history page
SELECT *
FROM table(information_schema.task_history())
ORDER BY scheduled_time DESC;

-- verify records loaded from "source" system
SELECT  'order' as tbl , count(distinct load_dts) as loads,  COUNT(*) cnt FROM src_orders 
GROUP BY 1 
UNION ALL 
SELECT  'customer' as tbl , count(distinct load_dts) as loads,  COUNT(*) cnt FROM src_customer
GROUP BY 1 ;



--------------------------------------------------------------------
-- create views for loading the Raw Vault
--------------------------------------------------------------------


CREATE OR REPLACE VIEW src_customer_strm_outbound AS 
SELECT 
-- source columns
*   
-- business key hash
     , SHA1_BINARY(UPPER(TRIM(c_custkey)))  hub_customer_hk     
-- record hash diff     
     , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( 
                                              NVL(TRIM(c_name)       ,'x')
                                            , NVL(TRIM(c_address)    ,'x')              
                                            , NVL(TRIM(iso2_code)    ,'x')                 
                                            , NVL(TRIM(c_phone)      ,'x')            
                                            , NVL(TRIM(c_acctbal)    ,'x')               
                                            , NVL(TRIM(c_mktsegment) ,'x')                 
                                            , NVL(TRIM(c_comment)    ,'x')               
                                            ), '^')))  AS customer_hash_diff
  FROM src_customer_strm src
;


CREATE OR REPLACE VIEW src_order_strm_outbound AS 
SELECT 
-- source columns
*   
-- business key hash
     , SHA1_BINARY(UPPER(TRIM(o_orderkey)))             hub_order_hk
     , SHA1_BINARY(UPPER(TRIM(o_custkey)))              hub_customer_hk  
     , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( NVL(TRIM(o_orderkey)       ,'x')
                                                        , NVL(TRIM(o_custkey)        ,'x')
                                                        ), '^')))  AS lnk_customer_order_hk
-- record hash diff                                                          
     , SHA1_BINARY(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( NVL(TRIM(o_orderstatus)    , 'x')         
                                                        , NVL(TRIM(o_totalprice)     , 'x')        
                                                        , NVL(TRIM(o_orderdate)      , 'x')       
                                                        , NVL(TRIM(o_orderpriority)  , 'x')           
                                                        , NVL(TRIM(o_clerk)          , 'x')    
                                                        , NVL(TRIM(o_shippriority)   , 'x')          
                                                        , NVL(TRIM(o_comment)        , 'x')      
                                                        ), '^')))  AS order_hash_diff     
  FROM src_orders_strm 
;





--------------------------------------------------------------------
-- set up Raw Vault
--------------------------------------------------------------------

USE SCHEMA L1_rdv;

-- create the hubs

CREATE OR REPLACE TABLE hub_customer
(
 hub_customer_hk  binary NOT NULL,
 c_custkey        number(38,0) NOT NULL,
 load_dts         timestamp_ntz(9) NOT NULL,
 rec_src          varchar(16777216) NOT NULL,

 CONSTRAINT pk_hub_customer PRIMARY KEY ( hub_customer_hk )
);                                    

CREATE OR REPLACE TABLE hub_order
(
 hub_order_hk  binary NOT NULL,
 o_orderkey    number(38,0) NOT NULL,
 load_dts      timestamp_ntz(9) NOT NULL,
 rec_src       varchar(16777216) NOT NULL,

 CONSTRAINT pk_hub_order PRIMARY KEY ( hub_order_hk )
);


-- create the ref table

CREATE OR REPLACE TABLE ref_nation
(
 iso2_code   varchar(2) NOT NULL,
 n_nationkey number(38,0) NOT NULL,
 n_regionkey number(38,0) NOT NULL,
 n_name      varchar(16777216),
 n_comment   varchar(16777216),
 load_dts    timestamp_ntz(9) NOT NULL,
 rec_src     varchar(16777216) NOT NULL,

 CONSTRAINT pk_ref_nation PRIMARY KEY ( iso2_code ),
 CONSTRAINT ak_ref_nation UNIQUE 	  ( n_nationkey )
)
AS 
SELECT 
	   iso2_code
	 , n_nationkey
     , n_regionkey
     , n_name
     , n_comment
     , load_dts
     , rec_src     
FROM L0_src.src_nation;


-- create the sats


CREATE OR REPLACE TABLE sat_sys1_customer
(
 hub_customer_hk  binary NOT NULL,
 load_dts         timestamp_ntz(9) NOT NULL,
 c_name           varchar(16777216),
 c_address        varchar(16777216),
 c_phone          varchar(16777216),
 c_acctbal        number(38,0),
 c_mktsegment     varchar(16777216),
 c_comment        varchar(16777216),
 iso2_code        varchar(2) NOT NULL,
 hash_diff        binary NOT NULL,
 rec_src          varchar(16777216) NOT NULL,

 CONSTRAINT pk_sat_sys1_customer PRIMARY KEY ( hub_customer_hk, load_dts ),
 CONSTRAINT fk_sat_sys1_customer_hcust FOREIGN KEY ( hub_customer_hk ) REFERENCES hub_customer ( hub_customer_hk ),
 CONSTRAINT fk_set_customer_rnation FOREIGN KEY ( iso2_code ) REFERENCES ref_nation ( iso2_code )
);                               




CREATE OR REPLACE TABLE sat_sys1_order
(
 hub_order_hk    binary NOT NULL,
 load_dts        timestamp_ntz(9) NOT NULL,
 o_orderstatus   varchar(16777216),
 o_totalprice    number(38,0),
 o_orderdate     date,
 o_orderpriority varchar(16777216),
 o_clerk         varchar(16777216),
 o_shippriority  number(38,0),
 o_comment       varchar(16777216),
 hash_diff       binary NOT NULL,
 rec_src         varchar(16777216) NOT NULL,

 CONSTRAINT pk_sat_sys1_order PRIMARY KEY ( hub_order_hk, load_dts ),
 CONSTRAINT fk_sat_sys1_order FOREIGN KEY ( hub_order_hk ) REFERENCES hub_order ( hub_order_hk )
);   

-- create the link

CREATE OR REPLACE TABLE lnk_customer_order
(
 lnk_customer_order_hk  binary NOT NULL,
 hub_customer_hk        binary NOT NULL,
 hub_order_hk           binary NOT NULL,
 load_dts               timestamp_ntz(9) NOT NULL,
 rec_src                varchar(16777216) NOT NULL,

 CONSTRAINT pk_lnk_customer_order PRIMARY KEY  ( lnk_customer_order_hk ),
 CONSTRAINT fk1_lnk_customer_order FOREIGN KEY ( hub_customer_hk ) REFERENCES hub_customer ( hub_customer_hk ),
 CONSTRAINT fk2_lnk_customer_order FOREIGN KEY ( hub_order_hk )    REFERENCES hub_order ( hub_order_hk )
);

  
 
--------------------------------------------------------------------
-- load the Raw Vault using multi-table insert
-------------------------------------------------------------------- 
  

 
CREATE OR REPLACE TASK customer_strm_tsk
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
WHEN
SYSTEM$STREAM_HAS_DATA('L0_SRC.SRC_CUSTOMER_STRM')
AS 
INSERT ALL
-- make sure record does not already exist in the hub
WHEN (SELECT COUNT(1) FROM hub_customer tgt WHERE tgt.hub_customer_hk = src_hub_customer_hk) = 0
THEN INTO hub_customer  
( hub_customer_hk
, c_custkey
, load_dts
, rec_src
)  
VALUES 
( src_hub_customer_hk
, src_c_custkey
, src_load_dts
, src_rec_src
) 
-- make sure record does not already exist in the sat
WHEN (SELECT COUNT(1) FROM sat_sys1_customer tgt WHERE tgt.hub_customer_hk = src_hub_customer_hk 
-- only insert if changes based on hash diff are detected
AND tgt.hash_diff = src_customer_hash_diff) = 0
THEN INTO sat_sys1_customer  
(
  hub_customer_hk  
, load_dts              
, c_name            
, c_address         
, c_phone           
, c_acctbal         
, c_mktsegment      
, c_comment         
, iso2_code        
, hash_diff         
, rec_src              
)  
VALUES 
(
  src_hub_customer_hk  
, src_load_dts              
, src_c_name            
, src_c_address         
, src_c_phone           
, src_c_acctbal         
, src_c_mktsegment      
, src_c_comment         
, src_iso2_code     
, src_customer_hash_diff         
, src_rec_src              
)
SELECT hub_customer_hk    src_hub_customer_hk
     , c_custkey           src_c_custkey
     , c_name              src_c_name
     , c_address           src_c_address
     , iso2_code           src_iso2_code
     , c_phone             src_c_phone
     , c_acctbal           src_c_acctbal
     , c_mktsegment        src_c_mktsegment
     , c_comment           src_c_comment    
     , customer_hash_diff  src_customer_hash_diff
     , load_dts            src_load_dts
     , rec_src             src_rec_src
  FROM l0_src.src_customer_strm_outbound src
;


CREATE OR REPLACE TASK order_strm_tsk
  WAREHOUSE = demo_wh
  SCHEDULE = '10 minute'
WHEN
  SYSTEM$STREAM_HAS_DATA('L0_SRC.SRC_ORDERS_STRM')
AS 
INSERT ALL
-- make sure record does not already exist in the hub
WHEN (SELECT COUNT(1) FROM hub_order tgt WHERE tgt.hub_order_hk = src_hub_order_hk) = 0
THEN INTO hub_order  
( hub_order_hk
, o_orderkey
, load_dts
, rec_src
)  
VALUES 
( src_hub_order_hk
, src_o_orderkey
, src_load_dts
, src_rec_src
)  
-- make sure record does not already exist in the sat
WHEN (SELECT COUNT(1) FROM sat_sys1_order tgt WHERE tgt.hub_order_hk = src_hub_order_hk 
-- only insert if changes based on hash diff are detected
AND tgt.hash_diff = src_order_hash_diff) = 0
THEN INTO sat_sys1_order  
(
  hub_order_hk  
, load_dts              
, o_orderstatus  
, o_totalprice   
, o_orderdate    
, o_orderpriority
, o_clerk        
, o_shippriority 
, o_comment              
, hash_diff         
, rec_src              
)  
VALUES 
(
  src_hub_order_hk  
, src_load_dts              
, src_o_orderstatus  
, src_o_totalprice   
, src_o_orderdate    
, src_o_orderpriority
, src_o_clerk        
, src_o_shippriority 
, src_o_comment      
, src_order_hash_diff         
, src_rec_src              
)
-- make sure record does not already exist in the link
WHEN (SELECT COUNT(1) FROM lnk_customer_order tgt WHERE tgt.lnk_customer_order_hk = src_lnk_customer_order_hk) = 0
THEN INTO lnk_customer_order  
(
  lnk_customer_order_hk  
, hub_customer_hk              
, hub_order_hk  
, load_dts
, rec_src              
)  
VALUES 
(
  src_lnk_customer_order_hk
, src_hub_customer_hk
, src_hub_order_hk  
, src_load_dts              
, src_rec_src              
)
SELECT hub_order_hk           src_hub_order_hk
     , lnk_customer_order_hk  src_lnk_customer_order_hk
     , hub_customer_hk        src_hub_customer_hk
     , o_orderkey              src_o_orderkey
     , o_orderstatus           src_o_orderstatus  
     , o_totalprice            src_o_totalprice   
     , o_orderdate             src_o_orderdate    
     , o_orderpriority         src_o_orderpriority
     , o_clerk                 src_o_clerk        
     , o_shippriority          src_o_shippriority 
     , o_comment               src_o_comment      
     , order_hash_diff         src_order_hash_diff
     , load_dts                    src_load_dts
     , rec_src                    src_rec_src
  FROM L0_src.src_order_strm_outbound src;    



 
  
-- audit the record counts before and after calling the RV load tasks  
SELECT 'hub_customer' src, count(1) cnt FROM hub_customer
UNION ALL
SELECT 'hub_order', count(1) FROM hub_order
UNION ALL
SELECT 'sat_sys1_customer', count(1) FROM sat_sys1_customer
UNION ALL
SELECT 'sat_sys1_order', count(1) FROM sat_sys1_order
UNION ALL
SELECT 'ref_nation' src, count(1) cnt FROM ref_nation
UNION ALL 
SELECT 'lnk_customer_order', count(1) FROM lnk_customer_order
UNION ALL
SELECT 'L0_src.src_customer_strm_outbound', count(1) FROM l0_src.src_customer_strm_outbound
UNION ALL
SELECT 'L0_src.src_order_strm_outbound', count(1) FROM l0_src.src_order_strm_outbound;  
;


EXECUTE TASK  customer_strm_tsk;
EXECUTE TASK  order_strm_tsk ;

-- load more source records and repeat the previous tasks to load them into the DV
EXECUTE  TASK  L0_src.load_daily_init;  


-- probe the task history programatically instead of using Snowsight UI
  SELECT *
  FROM table(information_schema.task_history())
  ORDER BY scheduled_time DESC;

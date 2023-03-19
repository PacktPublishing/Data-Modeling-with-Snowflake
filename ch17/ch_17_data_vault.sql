/*
 * 
 * Demystifying Data Vault
 * 
 * 
 */


--------------------------------------------------------------------
-- setting up environments
--------------------------------------------------------------------
CREATE DATABASE data_vault;

CREATE OR REPLACE SCHEMA L0_src COMMENT = 'Schema for landing area objects';

CREATE OR REPLACE SCHEMA L1_rdv COMMENT = 'Schema for Raw Vault objects';


--------------------------------------------------------------------
-- setting up the landing area
--------------------------------------------------------------------

USE SCHEMA L0_src;


CREATE OR REPLACE TABLE src_nation
(
 n_nationkey number(38,0) NOT NULL,
 n_name      varchar(25)  NOT NULL,
 n_regionkey number(38,0) NOT NULL,
 n_comment   varchar(152) ,
 load_dts    timestamp_ntz NOT NULL,
 rec_src     varchar NOT NULL,
 
 CONSTRAINT pk_src_nation PRIMARY KEY ( n_nationkey ),
 CONSTRAINT ak_src_nation UNIQUE ( n_name )
 )
 COMMENT = 'ISO 2-letter country codes'
AS 
SELECT *
     , CURRENT_TIMESTAMP()        
     , 'sys 1'    
  FROM snowflake_sample_data.tpch_sf10.nation
;

  
CREATE OR REPLACE TABLE src_customer
(
 c_custkey    number(38,0) NOT NULL,
 c_name       varchar(25),
 c_address    varchar(40),
 c_nationkey  number(38,0) NOT NULL,
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
	, c_nationkey
	, c_phone
	, c_acctbal
	, c_mktsegment
	, c_comment
	, current_timestamp()
	, 'sys 1'
FROM snowflake_sample_data.tpch_sf10.customer c 
INNER JOIN current_load l  ON c.c_custkey = l.custkey
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
     , MD5(UPPER(TRIM(c_custkey)))  md5_hub_customer     
-- record hash diff     
     , MD5(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( 
                                              NVL(TRIM(c_name)       ,'x')
                                            , NVL(TRIM(c_address)    ,'x')              
                                            , NVL(TRIM(c_nationkey)  ,'x')                 
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
     , MD5(UPPER(TRIM(o_orderkey)))             md5_hub_order
     , MD5(UPPER(TRIM(o_custkey)))              md5_hub_customer  
     , MD5(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( NVL(TRIM(o_orderkey)       ,'x')
                                                        , NVL(TRIM(o_custkey)        ,'x')
                                                        ), '^')))  AS md5_lnk_customer_order
-- record hash diff                                                          
     , MD5(UPPER(ARRAY_TO_STRING(ARRAY_CONSTRUCT( NVL(TRIM(o_orderstatus)    , 'x')         
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
  md5_hub_customer        varchar(32)    NOT NULL   
, c_custkey               number         NOT NULL
, load_dts                timestamp_ntz  NOT NULL
, rec_src                 varchar        NOT NULL

, CONSTRAINT pk_hub_customer        PRIMARY KEY(md5_hub_customer)
);                                     

CREATE OR REPLACE TABLE hub_order 
( 
  md5_hub_order           varchar(32)    NOT NULL   
, o_orderkey              number         NOT NULL               
, load_dts                timestamp_ntz  NOT NULL
, rec_src                 varchar        NOT NULL
, CONSTRAINT pk_hub_order                PRIMARY KEY(md5_hub_order)
);                                     


-- create the sats


CREATE OR REPLACE TABLE sat_customer 
( 
  md5_hub_customer       varchar(32)    NOT NULL   
, load_dts               timestamp_ntz  NOT NULL
, c_name                 varchar
, c_address              varchar
, c_phone                varchar 
, c_acctbal              number
, c_mktsegment           varchar    
, c_comment              varchar
, nationkey              number
, hash_diff              varchar(32)  	NOT NULL
, rec_src                varchar  		NOT NULL  

, CONSTRAINT pk_sat_customer     		PRIMARY KEY(md5_hub_customer, load_dts)
, CONSTRAINT fk_sat_customer     		FOREIGN KEY(md5_hub_customer) REFERENCES hub_customer
);                                     




CREATE OR REPLACE TABLE sat_order 
( 
  md5_hub_order          varchar(32)    NOT NULL   
, load_dts               timestamp_ntz  NOT NULL
, o_orderstatus          varchar   
, o_totalprice           number
, o_orderdate            date
, o_orderpriority        varchar
, o_clerk                varchar    
, o_shippriority         number
, o_comment              varchar
, hash_diff              varchar(32)    NOT NULL
, rec_src                varchar        NOT NULL 

, CONSTRAINT pk_sat_order PRIMARY KEY(md5_hub_order, load_dts)
, CONSTRAINT fk_sat_order FOREIGN KEY(md5_hub_order) REFERENCES hub_order
);   

-- create the links

CREATE OR REPLACE TABLE lnk_customer_order
(
  md5_lnk_customer_order  varchar(32)     NOT NULL   
, md5_hub_customer        varchar(32) 
, md5_hub_order           varchar(32) 
, load_dts                timestamp_ntz   NOT NULL
, rec_src                 varchar         NOT NULL  

, CONSTRAINT pk_lnk_customer_order  PRIMARY KEY(md5_lnk_customer_order)
, CONSTRAINT fk1_lnk_customer_order FOREIGN KEY(md5_hub_customer) REFERENCES hub_customer
, CONSTRAINT fk2_lnk_customer_order FOREIGN KEY(md5_hub_order)    REFERENCES hub_order
);



-- create the ref table

CREATE OR REPLACE TABLE ref_nation 
( 
  nation_id             number 
, region_id             number 
, load_dts              timestamp_ntz   NOT NULL
, rec_src               varchar         NOT NULL
, n_name                varchar
, n_comment             varchar

, CONSTRAINT pk_ref_nation PRIMARY KEY (nation_id)   
)
AS 
SELECT n_nationkey
     , n_regionkey
     , load_dts
     , rec_src
     , n_name
     , n_comment
  FROM L0_src.src_nation;
  
  
  
  
  
 
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
WHEN (SELECT COUNT(1) FROM hub_customer tgt WHERE tgt.md5_hub_customer = src_md5_hub_customer) = 0
THEN INTO hub_customer  
( md5_hub_customer
, c_custkey
, load_dts
, rec_src
)  
VALUES 
( src_md5_hub_customer
, src_c_custkey
, src_load_dts
, src_rec_src
) 
-- make sure record does not already exist in the sat
WHEN (SELECT COUNT(1) FROM sat_customer tgt WHERE tgt.md5_hub_customer = src_md5_hub_customer 
-- only insert if changes based on hash diff are detected
AND tgt.hash_diff = src_customer_hash_diff) = 0
THEN INTO sat_customer  
(
  md5_hub_customer  
, load_dts              
, c_name            
, c_address         
, c_phone           
, c_acctbal         
, c_mktsegment      
, c_comment         
, nationkey        
, hash_diff         
, rec_src              
)  
VALUES 
(
  src_md5_hub_customer  
, src_load_dts              
, src_c_name            
, src_c_address         
, src_c_phone           
, src_c_acctbal         
, src_c_mktsegment      
, src_c_comment         
, src_nationkey        
, src_customer_hash_diff         
, src_rec_src              
)
SELECT md5_hub_customer    src_md5_hub_customer
     , c_custkey           src_c_custkey
     , c_name              src_c_name
     , c_address           src_c_address
     , c_nationkey         src_nationkey
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
WHEN (SELECT COUNT(1) FROM hub_order tgt WHERE tgt.md5_hub_order = src_md5_hub_order) = 0
THEN INTO hub_order  
( md5_hub_order
, o_orderkey
, load_dts
, rec_src
)  
VALUES 
( src_md5_hub_order
, src_o_orderkey
, src_load_dts
, src_rec_src
)  
-- make sure record does not already exist in the sat
WHEN (SELECT COUNT(1) FROM sat_order tgt WHERE tgt.md5_hub_order = src_md5_hub_order 
-- only insert if changes based on hash diff are detected
AND tgt.hash_diff = src_order_hash_diff) = 0
THEN INTO sat_order  
(
  md5_hub_order  
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
  src_md5_hub_order  
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
WHEN (SELECT COUNT(1) FROM lnk_customer_order tgt WHERE tgt.md5_lnk_customer_order = src_md5_lnk_customer_order) = 0
THEN INTO lnk_customer_order  
(
  md5_lnk_customer_order  
, md5_hub_customer              
, md5_hub_order  
, load_dts
, rec_src              
)  
VALUES 
(
  src_md5_lnk_customer_order
, src_md5_hub_customer
, src_md5_hub_order  
, src_load_dts              
, src_rec_src              
)
SELECT md5_hub_order           src_md5_hub_order
     , md5_lnk_customer_order  src_md5_lnk_customer_order
     , md5_hub_customer        src_md5_hub_customer
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
SELECT 'sat_customer', count(1) FROM sat_customer
UNION ALL
SELECT 'sat_order', count(1) FROM sat_order
UNION ALL
SELECT 'lnk_customer_order', count(1) FROM lnk_customer_order
UNION ALL
SELECT 'L0_src.src_customer_strm_outbound', count(1) FROM l0_src.src_customer_strm_outbound
UNION ALL
SELECT 'L0_src.src_order_strm_outbound', count(1) FROM l0_src.src_order_strm_outbound;  
;


EXECUTE TASK  customer_strm_tsk;
EXECUTE TASK  order_strm_tsk ;

-- load more source records to repeat the process
EXECUTE  TASK  l0_src.load_daily_init;  


  SELECT *
  FROM table(information_schema.task_history())
  ORDER BY scheduled_time DESC;

CREATE OR REPLACE SCHEMA ch13_dims; 


CREATE OR REPLACE TABLE source_system_customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'user comments',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id ) 
)
COMMENT = 'loaded from snowflake_sample_data.tpch_sf10.customer'
AS 
SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment  
FROM snowflake_sample_data.tpch_sf10.customer
;

CREATE OR REPLACE TABLE src_customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'base load of one fourth of total records',
 __ldts              timestamp_ntz NOT NULL DEFAULT current_timestamp(),

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id, __ldts )
)
COMMENT = 'source customers for loading changes'
as 
SELECT customer_id
	, name
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , current_timestamp()
FROM source_system_customer
WHERE true 
AND MOD(customer_id,4)= 0 --load one quarter of existing recrods
;

--create a clone of src_customer for future exercises
CREATE OR REPLACE src_customer_bak CLONE src_customer; 


CREATE OR REPLACE TASK load_src_customer
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
AS 
INSERT INTO src_customer (
SELECT	customer_id
	, name
	, address
	, location_id
	, phone
    --if customer id ends in 3, vary the balance amount
    , iff(mod(customer_id,3)= 0,  (account_balance_usd+random()/100000000000000000)::number(32,2), account_balance_usd)
	, market_segment
	, comment
    , current_timestamp()
FROM operations.customer SAMPLE (1000 ROWS)
)
;



/*
* Type 1 
*/

--create base table
CREATE OR REPLACE TABLE dim_customer_t1
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'base load of one fourth of total records',
 diff_hash			 varchar (32) NOT NULL,
 __ldts              timestamp_ntz NOT NULL DEFAULT current_timestamp() COMMENT 'load date of latest source record',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id )
)
COMMENT = 'type 1 customer dim'
as 
SELECT customer_id
	, name	
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , MD5(account_balance_usd) --hash type 1 attributes for easy compare
    , __ldts
    FROM src_customer;


--simulate a source load
EXECUTE TASK load_src_customer ;


--load type 1
MERGE INTO dim_customer_t1 dc
--get only latest recrods from src_customer. In a real-world scenario,
--create a view to get the latest records to make the logic leaner
USING (SELECT *, MD5(account_balance_usd) AS diff_hash 
		FROM src_customer  WHERE __ldts =  (SELECT MAX(__ldts) FROM src_customer)
      ) sc
ON dc.customer_id = sc.customer_id
WHEN NOT MATCHED --new records, insert
THEN INSERT VALUES (
	  customer_id
	, name
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , diff_hash
    , __ldts
)
WHEN MATCHED --record exists
AND dc.diff_hash != sc.diff_hash --check for changes in T.1 dim
THEN UPDATE 
SET   dc.account_balance_usd  = sc.account_balance_usd
	, dc.__ldts = sc.__ldts --to indicate when last updated
	, dc.diff_hash = sc.diff_hash
;


/*
* Type 2 
*/

--reset source table
CREATE OR REPLACE TABLE src_customer CLONE src_customer_bak;


--create base table
CREATE OR REPLACE TABLE dim_customer_t2
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'base load of one fourth of total records',
 --using timestamps for from/to columns because this example will perform
 --multiple loads in a given day. Most business scenarios would use a date type column. 
 from_dts			 timestamp_ntz NOT NULL,
 to_dts			     timestamp_ntz NOT NULL,
 diff_hash			 varchar (32) NOT NULL,

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id, from_dts )
)
COMMENT = 'type 2 customer dim'
as 
SELECT customer_id
	, name	
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , __ldts --from_date
    , '9999-12-31'::timestamp_ntz --to_date
    , MD5(account_balance_usd) --hash type 2 attributes for easy compare
    FROM src_customer;



--create a stream on the t2 dim
CREATE OR REPLACE STREAM strm_dim_customer_t2 ON TABLE dim_customer_t2;

--simulate a source load
EXECUTE TASK load_src_customer ;


--load type 2
--step 1 (very similar to type 1 SCD merge)
MERGE INTO dim_customer_t2 dc
--get only latest records from src_customer. In a real-world scenario,
--create a view to get the latest records to make the logic leaner
USING (SELECT *, MD5(account_balance_usd) AS diff_hash 
		FROM src_customer  WHERE __ldts =  (SELECT MAX(__ldts) FROM src_customer)
      ) sc
ON dc.customer_id = sc.customer_id
AND  dc.to_dts = '9999-12-31'
WHEN NOT MATCHED --new records, insert
THEN INSERT VALUES (
	  customer_id
	, name	
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , __ldts --from_date
    , '9999-12-31'::timestamp_ntz --to_date
    , MD5(account_balance_usd) --hash type 2 attributes for easy compare
)
WHEN MATCHED --record exists
AND dc.diff_hash != sc.diff_hash --check for changes in T.2 dim
THEN UPDATE 
SET   dc.account_balance_usd  = sc.account_balance_usd
	, dc.from_dts = sc.__ldts  --update the from date to the latest load
	, dc.diff_hash = sc.diff_hash
;


--load type 2
--step 2 (update metadata in updated t.2 attributes)
INSERT INTO dim_customer_t2 
SELECT 
      customer_id
    , name	
    , address
    , location_id
    , phone
    , account_balance_usd
    , market_segment
    , comment
    , from_dts 						--original from date
    , dateadd(second,-1, new_to_dts)  --delimit new to_date to be less than inserted from_dts
    , diff_hash
FROM strm_dim_customer_t2 strm 
INNER JOIN   ((SELECT MAX(__ldts) as new_to_dts FROM src_customer)) --get the to_dts for current load
ON true 
AND strm.metadata$action = 'DELETE'  --get before-image for updated records 
WHERE true
;

--recreate the stream
--because it now contains the inserted (updated) records
--this step is optional because our logic filters on 
-- strm.metadata$action = 'DELETE', but it's cleaner
CREATE OR REPLACE STREAM strm_dim_customer_t2 ON TABLE dim_customer_t2;




--embed steps 1 and 2 into a task tree for easy loading
CREATE OR REPLACE TASK tsk_load_dim_customer_t2
WAREHOUSE = demo_wh
SCHEDULE = '10 minute'
AS SELECT true
;


--create merge task (step 1)
CREATE OR REPLACE TASK tsk_load_1_dim_customer_t2
WAREHOUSE = demo_wh
AFTER tsk_load_dim_customer_t2
AS 
MERGE INTO dim_customer_t2 dc
--get only latest recrods from src_customer. In a real-world scenario,
--create a view to get the latest records to make the logic leaner
USING (SELECT *, MD5(account_balance_usd) AS diff_hash 
		FROM src_customer  WHERE __ldts =  (SELECT MAX(__ldts) FROM src_customer)
      ) sc
ON dc.customer_id = sc.customer_id
AND  dc.to_dts = '9999-12-31'
WHEN NOT MATCHED --new records, insert
THEN INSERT VALUES (
	  customer_id
	, name	
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , __ldts --from_date
    , '9999-12-31'::timestamp_ntz --to_date
    , MD5(account_balance_usd) --hash type 2 attributes for easy compare
)
WHEN MATCHED --record exists
AND dc.diff_hash != sc.diff_hash --check for changes in T.2 dim
THEN UPDATE 
SET   dc.account_balance_usd  = sc.account_balance_usd
	, dc.from_dts = sc.__ldts  --update the from date to the latest load
	, dc.diff_hash = sc.diff_hash
;

ALTER TASK tsk_load_1_dim_customer_t2 resume;

--create the insert task (step 2)
CREATE OR REPLACE TASK tsk_load_2_dim_customer_t2
WAREHOUSE = demo_wh
AFTER tsk_load_1_dim_customer_t2
AS 
INSERT INTO dim_customer_t2 
SELECT 
      customer_id
    , name	
    , address
    , location_id
    , phone
    , account_balance_usd
    , market_segment
    , comment
    , from_dts 						--original from date
    , dateadd(second,-1, new_to_dts)  --delimit new to_date to be less than inserted from_dts
    , diff_hash
FROM strm_dim_customer_t2 strm 
INNER JOIN   ((SELECT MAX(__ldts) as new_to_dts FROM src_customer)) --get the to_dts for current load
ON true 
AND strm.metadata$action = 'DELETE'  --get before-image for updated records 
WHERE true
;

ALTER TASK tsk_load_2_dim_customer_t2 resume;

--simulate a source load
EXECUTE TASK load_src_customer ;

--load t.2 dim
EXECUTE TASK tsk_load_dim_customer_t2 ;





/*
* Type 3
*/
--reset source table
CREATE OR REPLACE TABLE src_customer CLONE src_customer_bak;

--create base table
--before introducing a t.3 dimension in this example,
--the table resembles a t.1
CREATE OR REPLACE TABLE dim_customer_t3
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'base load of one fourth of total records',
 diff_hash			 varchar (32) NOT NULL,
 __ldts              timestamp_ntz NOT NULL DEFAULT current_timestamp() COMMENT 'load date of latest source record',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id )
)
COMMENT = 'type 1 customer dim'
as 
SELECT customer_id
	, name	
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , MD5(account_balance_usd) --hash type 1 attributes for easy compare
    , __ldts
    FROM src_customer;



--add a type 3 dimension
ALTER TABLE dim_customer_t3 ADD COLUMN original_account_balance_usd number(12,2);

UPDATE dim_customer_t3
SET original_account_balance_usd = account_balance_usd;

--simulate a source load
EXECUTE TASK load_src_customer ;


--load type 1 in t3
MERGE INTO dim_customer_t3 dc
--get only latest recrods from src_customer. In a real-world scenario,
--create a view to get the latest records to make the logic leaner
USING (SELECT *, MD5(account_balance_usd) AS diff_hash 
		FROM src_customer  WHERE __ldts =  (SELECT MAX(__ldts) FROM src_customer)
      ) sc
ON dc.customer_id = sc.customer_id
WHEN NOT MATCHED --new records, insert
THEN INSERT VALUES (
	  customer_id
	, name
	, address
	, location_id
	, phone
    , account_balance_usd
	, market_segment
	, comment
    , diff_hash
    , __ldts
    , account_balance_usd --this is the T.3 column and will not be updated going forward
)
WHEN MATCHED --record exists
AND dc.diff_hash != sc.diff_hash --check for changes in T.1 dim
THEN UPDATE 
SET   dc.account_balance_usd  = sc.account_balance_usd
	, dc.__ldts = sc.__ldts --to indicate when last updated
	, dc.diff_hash = sc.diff_hash
;


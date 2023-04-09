/*
 *
 * Virtual columns
 * ---------------
 * The secret feature that sits between
 * physical and transformational modeling.
 * 
 */
 



--------------------------------------------------------------------
-- setting up environments
--------------------------------------------------------------------
CREATE OR REPLACE SCHEMA secret_virtual_columns; 


-- Create a table with physical, virtual, and DEFAULT columns
CREATE OR REPLACE TRANSIENT TABLE default_v_virtual_demo  
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 --create a virtual column as an expression
 v_Name				 varchar NOT NULL AS ('Hi, my name is '|| NAME), --'AS' syntax is not in documentation as-at 2023-04-08 but supported
 --create the same column as a DEFAULT and as a virtual column 
 load_dts 			 timestamp_ltz DEFAULT  CURRENT_TIMESTAMP(),
 v_Load_dts 		 timestamp_ltz AS CURRENT_TIMESTAMP() 
)


-- Insert data into the table
INSERT INTO default_v_virtual_demo 
(customer_id, name, load_dts )
VALUES 
--virtual columns are transformational and do not store physical data
--to insert a default, use the 'DEFAULT' keyword, otherwise specify the value to be inserted
(1,'Serge',DEFAULT ),
(2,'Bill', NULL)
;



SELECT * FROM default_v_virtual_demo;

-- Notice that DEFAULT columns are static, while virtual columns are dynamic (LOAD_DTS vs V_LOAD_DTS)
/*
 * 
CUSTOMER_ID|NAME |V_NAME              |LOAD_DTS               |V_LOAD_DTS             |
-----------+-----+--------------------+-----------------------+-----------------------+
          1|Serge|Hi, my name is Serge|2023-04-09 12:33:20.257|2023-04-09 12:33:24.727|
          2|Bill |Hi, my name is Bill |                       |2023-04-09 12:33:24.727|
 *          
 */


-- Notice that virtual columns are stored differently from physical
DESC TABLE default_v_virtual_demo;


-- Same info in the information schema
SELECT * FROM information_schema.columns
WHERE TRUE 
AND TABLE_SCHEMA = 'SECRET_VIRTUAL_COLUMNS';



-- Sample usage of virtual columns in physical table creation
CREATE OR REPLACE TRANSIENT TABLE customer
(
--add a message without using storage
 sys_message		 varchar AS 'Legacy data, do not use in reporting without remapping',
 customer_id         number(38,0) NOT NULL,
--add basic business rules without duplicating data 
 legacy_cust_id		 varchar AS ('x' || customer_id) COMMENT 'legacy system included an X prefix for cust IDs',
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
--add more advanced business rules without using storage
 tax_amount_usd      number(12,2) NOT NULL  AS (
				 		CASE  WHEN market_segment = 'MACHINERY'  THEN account_balance_usd * .1 
						WHEN market_segment = 'AUTOMOBILE' THEN account_balance_usd * .15
						ELSE account_balance_usd * .2 END 
						) ,  
 comment             varchar COMMENT 'user comments',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id ) 
)
COMMENT = 'loaded from snowflake_sample_data.tpch_sf10.customer'
AS 
--notice that virtual columns are ignored in the insert column order
SELECT c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment  
FROM snowflake_sample_data.tpch_sf10.customer SAMPLE (1000 rows)
;


-- Review the resulting values
SELECT * FROM customer;

-- Notice that the expressions are preserved in the DDL, albeit in a slightly
-- different format after being translated by the Snowflake query optimizer
SELECT GET_DDL ('table', 'customer');



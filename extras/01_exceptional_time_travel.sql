/*
 * 
 * The exceptional time traveler 
 * who detects table changes.
 * 
 * 
 */
 
--------------------------------------------------------------------
-- setting up environments
--------------------------------------------------------------------

CREATE OR REPLACE SCHEMA time_travel_except; 

CREATE OR REPLACE TRANSIENT TABLE customer
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




--------------------------------------------------------------------
-- perform a random/complex update 
--------------------------------------------------------------------


-- this example will fix the "before" timestamp in a variable for demonstration purposes.
-- normally, the timestamp or query id would be determined from the query history, based on
-- the time/query when the update was performed.
set before_time = CURRENT_TIMESTAMP();


UPDATE customer 
SET account_balance_usd = account_balance_usd + 1  
WHERE TRUE 
AND RIGHT(customer_id,4) = RIGHT(RANDOM(),4); 




-- Recall the SELECT syntax, including time travel options
/*
 * 
	SELECT ...
	FROM ...
	  {
	   AT( { TIMESTAMP => <timestamp> | OFFSET => <time_difference> | STATEMENT => <id> | STREAM => '<name>' } ) |
	   BEFORE( STATEMENT => <id> )
	  }
	[ ... ]
 *
*/





-- Recall the Snowflake set operators
-- { INTERSECT | { MINUS | EXCEPT } | UNION [ ALL ] } 


-- count how many records were updated
-- by selecting the PK and changed column from the original table using time travel
-- and comparing it to the current version
SELECT COUNT(*) cnt FROM (
SELECT customer_id, account_balance_usd FROM customer  AT(TIMESTAMP => $before_time) cust_before 
EXCEPT 
SELECT customer_id, account_balance_usd FROM customer cust_now
);


-- get the before and after values side by side
WITH 
--get the list of changed PKs
updated AS ( 
SELECT customer_id, account_balance_usd FROM customer  AT(TIMESTAMP => $before_time) cust_before 
EXCEPT 
SELECT customer_id, account_balance_usd FROM customer cust_now
)
--get the before values
, original AS (
SELECT customer_id, account_balance_usd FROM customer  AT(TIMESTAMP => $before_time) 
)
--get the after/now values
, now AS (
SELECT customer_id, account_balance_usd FROM customer 
)
--join the before and after values to see them side by side
SELECT o.customer_id 
, o.account_balance_usd AS acct_bal_original
, n.account_balance_usd AS acct_bal_now
FROM original o
INNER JOIN now n
USING (customer_id)
INNER JOIN updated u
USING (customer_id)


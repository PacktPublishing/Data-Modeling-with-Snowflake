CREATE TABLE loyalty_customer
(
 customer_id   number(38,0) NOT NULL,
 level         varchar NOT NULL COMMENT 'customer full name',
 type          varchar NOT NULL COMMENT 'loyalty tier: bronze, silver, or gold',
 points_amount number NOT NULL,
 comment       varchar COMMENT 'customer loyalty status calculated from sales order volume',

 CONSTRAINT pk_loyalty_customer PRIMARY KEY ( customer_id ) RELY,
 CONSTRAINT fk_loyalty_customer FOREIGN KEY ( customer_id ) REFERENCES customer ( customer_id ) RELY
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



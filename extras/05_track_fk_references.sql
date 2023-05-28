/*
 *
 * Tracking FK references
 * ---------------
 * Foreign Key references are not recorded 
 * in the information schema. Find out how to 
 * track them with provided Snowflake functions
 * or even populate your own metadata table
 * 
 */
 

/*
 * Thank you to Saqib Ali
 * https://sql.yt/show-imported-keys.html
 * 
 */


-- Create tables with Foreign Keys
CREATE OR REPLACE SCHEMA fkref;

CREATE OR REPLACE TABLE customers(
  customer_id INT
  , name STRING
  , CONSTRAINT primary_key PRIMARY KEY (customer_id)
);
  
CREATE OR REPLACE TABLE customer_address(
  customer_id INT
  , name STRING
  , CONSTRAINT primary_key PRIMARY KEY (customer_id)
  , CONSTRAINT customer_foreing_key FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE OR REPLACE TABLE items(
  item_id INT
  , item_description STRING
  , CONSTRAINT primary_key PRIMARY KEY (item_id)
);
  
CREATE OR REPLACE TABLE orders(
  order_id INT
  , customer_id INT
  , item_id INT
  , CONSTRAINT primary_key PRIMARY KEY (order_id)
  , CONSTRAINT customer_foreing_key FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
  , CONSTRAINT item_foreign_key FOREIGN KEY (item_id) REFERENCES items(item_id)
);


/* 

*/

/*
 * Query the constraint metadata
 * 
 * this exercise will use the following functions:
 * 
 * SHOW IMPORTED KEYS; 
 * SHOW EXPORTED KEYS;
 * SHOW PRIMARY KEYS;
 *
 * Although import/export keys are not documented in Snowflake documentation 
 * as of 2023-05-28, their parameters are identical to SHOW PRIMARY KEYS:
 * https://docs.snowflake.com/en/sql-reference/sql/show-primary-keys
 */


-- show primary keys for a table 
SHOW PRIMARY KEYS IN orders;

-- show foreign keys referenced by a given table
SHOW IMPORTED KEYS IN orders; 

-- show where a table is referenced as a FK 
SHOW EXPORTED KEYS IN items;


/*
 * For a more advanced use case, refer to this article
 * by Dan Linstedt from DataVaultAlliance that creates a procedure
 * using these functions to populate a table with the results. This way
 * the results can be recorded with an account with SHOW privileges
 * and SELECT access to the table can be restricted to less privileged users.
 * 
 * https://datavaultalliance.com/news/snowflakegetting-foreign-key-columns/
 */

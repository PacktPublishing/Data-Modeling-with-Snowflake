/*
 * 
 * 
 * 13.3. Time banded facts using a Type 2 structure
 * 
 * 
 */



---------------------------------------------------------------------------------------------------------------------
-- Prepare the base tables
---------------------------------------------------------------------------------------------------------------------

SET today = '1995-12-01';


--create "source system" sample data
CREATE OR REPLACE TABLE source_system_employee
(
 employee_id  number(38,0) NOT NULL,
 depatment_id     number(38,0) NOT NULL,
 employee_type    varchar(1) NOT NULL,
 salary_usd number(12,2) NOT NULL,
 hire_date      date NOT NULL,
 security_clearance  varchar(15) NOT NULL,
 clerk           varchar(15) NOT NULL,
 comment         varchar(79) COMMENT 'convert snowflake_sample_data.tpch_sf10.orders to employees',

 CONSTRAINT pk_source_system_employee PRIMARY KEY ( employee_id ) 
)
COMMENT = 'Employee sample data'
AS 
SELECT o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, o_orderpriority, o_clerk, o_comment  
FROM snowflake_sample_data.tpch_sf10.orders 
;


--create initial load of day 1  DWH landing area
CREATE OR REPLACE TABLE src_employee 
(
employee_id NUMBER(38,0) NOT NULL
, depatment_id NUMBER(38,0) NOT NULL
, is_contractor boolean NOT NULL
, salary_usd NUMBER(12,2) NOT NULL
, hire_date DATE NOT NULL
, termination_date DATE  NOT NULL
, security_clearance VARCHAR(15) NOT NULL
, clerk VARCHAR(15) NOT NULL
, is_active boolean  NOT NULL
, last_change varchar NOT NULL 
, comment VARCHAR NOT NULL
, __load_date date NOT NULL
, CONSTRAINT pk_src_employee PRIMARY KEY ( employee_id , __load_date) 
)
COMMENT = 'Base table load at day 1'
AS 
SELECT
	  employee_id 
	, depatment_id
	, RIGHT(clerk,1)::int > 7 --IS contractor
	, salary_usd 
	, hire_date
	, CASE WHEN   RIGHT(clerk,1)::int < 3 
	 	THEN '9999-12-31'::date 
		ELSE dateadd( 'DAY' , RIGHT(salary_usd::integer,3), hire_date) END  AS termination_date 
	, security_clearance 
	, clerk 
	, IFF( termination_date >= $today, TRUE, FALSE) AS is_active
	, IFF( termination_date >= $today, 'Hire', 'Leaver') AS is_active
	, COMMENT
	, $today 
FROM 	source_system_employee  SAMPLE (50000 rows)
WHERE TRUE 
AND hire_date < $today
;



CREATE OR REPLACE TABLE employee_t2 ( 
	employee_id NUMBER(38,0) NOT NULL
	, depatment_id NUMBER(38,0) NOT NULL
	, is_contractor boolean NOT NULL
	, salary_usd NUMBER(12,2) NOT NULL
	, hire_date DATE NOT NULL
	, termination_date DATE NOT NULL
	, security_clearance VARCHAR(15) NOT NULL
	, clerk VARCHAR(15) NOT NULL
	, is_active boolean  NOT NULL
	, last_change varchar NOT NULL 
	, comment VARCHAR()  NOT NULL COMMENT 'HR feedback'
	, __load_date date COMMENT 'load date from src_employee'
	, from_date date NOT NULL
	, to_date date NOT NULL
	, diff_hash varchar(32) NOT NULL
	, CONSTRAINT pk_src_employee PRIMARY KEY ( employee_id, from_date ) 
)
COMMENT = 'Create and instantiate employee fact table at day 1'
AS  
SELECT employee_id
, depatment_id
, is_contractor
, salary_usd
, hire_date
, termination_date
, security_clearance
, clerk
, is_active
, last_change
, comment
, __load_date --src_load_date
, '1900-01-01'-- from_date
, '9999-12-31' --TO date
, MD5(salary_usd||hire_date||termination_date||comment||last_change||__load_date||is_active)
FROM
	src_employee
;

--create backups for re-running the exercise
CREATE OR REPLACE TABLE src_employee_bak CLONE src_employee;
CREATE OR REPLACE TABLE src_employee CLONE src_employee_bak;

CREATE OR REPLACE STREAM strm_employee_t2 ON TABLE employee_t2;



---------------------------------------------------------------------------------------------------------------------
-- Simulate a daily load
---------------------------------------------------------------------------------------------------------------------
SET today = $today::date+1;

SELECT $today;


INSERT INTO src_employee 
--get changes
WITH existing AS ( 
SELECT *
, iff(right(salary_usd,1)::int > 6, 'leaver', 'update') AS update_type 
FROM 	src_employee  SAMPLE (150 rows)
WHERE TRUE 
AND is_active
AND hire_date < $today::date-2 
)
--load 100 new hires 
SELECT
	  source_system_employee.employee_id 
	, depatment_id
	, RIGHT(clerk,1)::int > 7 --IS contractor
	, salary_usd 
	, hire_date
	, CASE WHEN   RIGHT(clerk,1)::int <= 7
	 	THEN '9999-12-31'::date 
		ELSE dateadd( 'DAY' , RIGHT(salary_usd::integer,3), hire_date) END  AS termination_date 
	, security_clearance 
	, clerk 
	, TRUE 
	, 'Hire'
	, COMMENT
	, $today
FROM 	source_system_employee  sample(100 rows)
WHERE TRUE 
AND hire_date = $today::date-1 
UNION ALL
--load  leavers
SELECT
	  employee_id 
	, depatment_id
	, is_contractor
	, salary_usd 
	, hire_date 
	, $today::date-1
	, security_clearance 
	, clerk 
	, FALSE 
	, 'Leaver'
	, 'Left '||comment
	, $today
FROM 	existing
WHERE TRUE 
AND update_type = 'leaver'
UNION ALL
--load existing emp updates
SELECT
	  employee_id 
	, depatment_id
	, is_contractor
	, salary_usd * 1.1 --10% raise            
	, hire_date
	, termination_date
	, security_clearance 
	, clerk 
	, is_active
	, 'Promoted'
	, '+10% '||comment
	, $today
FROM 	existing
WHERE TRUE 
AND update_type = 'update'
;


MERGE INTO employee_t2 tgt
--get only latest records from src_employee.
USING (SELECT *, MD5(salary_usd||hire_date||termination_date||comment||last_change||__load_date||is_active) AS diff_hash 
		--get the max load date from target and load next date from source
		FROM src_employee  WHERE __load_date =  (SELECT MAX(__load_date)::date+1 FROM employee_t2)
      ) src
ON tgt.employee_id = src.employee_id
AND  tgt.to_date = '9999-12-31'
WHEN NOT MATCHED --new records, insert
THEN INSERT VALUES (
	  employee_id
	, depatment_id
	, is_contractor
	, salary_usd
	, hire_date
	, termination_date
	, security_clearance
	, clerk
	, is_active
	, last_change
	, comment
	, __load_date
	, __load_date  -- from_date
	, '9999-12-31' -- to date
	, diff_hash
)
WHEN MATCHED --record EXISTS
--this example uses source delta loads but real-world could be full, 
--so need to check the diff_hash to determine if changes happened
AND tgt.diff_hash != src.diff_hash --check for changes in T.2 dim
THEN UPDATE 
SET   tgt.salary_usd			= src.salary_usd
	, tgt.hire_date			= src.hire_date
	, tgt.termination_date	= src.termination_date
	, tgt.COMMENT				= src.comment
	, tgt.last_change			= src.last_change
	, tgt.from_date			= src.__load_date
	, tgt.is_active			= src.is_active
	, tgt.__load_date			= src.__load_date
	, tgt.diff_hash = MD5(src.salary_usd||src.hire_date||src.termination_date||src.comment||src.last_change||src.__load_date||src.is_active)
;	


--insert the original value from stream
INSERT INTO employee_t2 
SELECT 
		employee_id
	, depatment_id
	, is_contractor
	, salary_usd
	, hire_date
	, termination_date
	, security_clearance
	, clerk
	, is_active
	, last_change
	, comment
	, __load_date
    , from_date 						--original from date
    , dateadd(day,-1, new_to_date)  --delimit new to_date to be less than inserted from_dts
    , diff_hash
FROM strm_employee_t2 strm 
INNER JOIN   ((SELECT MAX(__load_date) as new_to_date FROM employee_t2)) --get the to_date for current load
ON true 
AND strm.metadata$action = 'DELETE'  --get before-image for updated records 
WHERE TRUE
;


---------------------------------------------------------------------------------------------------------------------
-- Experiment with queries that pivot on varying intervals
---------------------------------------------------------------------------------------------------------------------

--currently active employees 
SELECT COUNT(*) cnt FROM employee_t2 
WHERE TRUE 
AND is_active 
AND to_date = '9999-12-31'  --currently
;


--active employees on day 1995-12-01 
SELECT COUNT(DISTINCT employee_id) cnt 
FROM employee_t2 
WHERE TRUE 
AND is_active
AND from_date <= '1995-12-01'  
AND to_date >= '1995-12-01'    
;


--active employees in all of 1995
SELECT COUNT(DISTINCT employee_id) cnt FROM employee_t2 
WHERE TRUE
AND is_active 
AND YEAR(from_date) <= 1995
AND YEAR(to_date) >= 1995
;


--active employees on day 1995-12-01 
--who were hired in Q1 of 1994 
SELECT COUNT(DISTINCT employee_id) cnt 
FROM employee_t2 
WHERE TRUE 
AND is_active
AND hire_date BETWEEN '1994-01-01' AND '1994-03-31'
AND from_date <= '1995-12-01'  
AND to_date >= '1995-12-01' ; 




--active employees on day 1995-12-01 
--who were hired in Q1 of 1994 
--and received a promotion
WITH promotions AS (
SELECT DISTINCT employee_id FROM employee_t2
WHERE TRUE 
AND last_change = 'Promoted'
)
SELECT COUNT(DISTINCT employee_id) cnt 
FROM employee_t2
INNER JOIN promotions USING (employee_id)
WHERE TRUE 
AND is_active
AND hire_date BETWEEN '1994-01-01' AND '1994-03-31'
AND from_date <= '1995-12-01'  
AND to_date >= '1995-12-01' ; 



--what are the total changes per day by change type
--since the first load ( excluding 1995-12-01)
SELECT from_date, last_change,  COUNT( employee_id) cnt 
FROM employee_t2 
WHERE TRUE 
AND from_date > '1995-12-01'  
AND to_date = '9999-12-31'  --currently
GROUP BY 1,2
ORDER BY 1,2;


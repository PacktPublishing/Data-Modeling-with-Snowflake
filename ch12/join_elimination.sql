CREATE OR REPLACE SCHEMA rely_test;

-- ************************************** region
CREATE  TABLE region
(
 region_id number(38,0) NOT NULL,
 name      varchar(25),
 comment   varchar(152) COMMENT 'VARCHAR(152)',

 CONSTRAINT pk_region PRIMARY KEY ( region_id ) RELY
)
COMMENT = 'Country business groupings'
AS 
SELECT * FROM snowflake_sample_data.tpch_sf10.region
;

-- ************************************** main_region
CREATE  TABLE region_main
(
 region_id number(38,0) NOT NULL,
 name      varchar(25),
 comment   varchar(152) COMMENT 'VARCHAR(152)',

 CONSTRAINT pk_region_main PRIMARY KEY ( region_id ) RELY  
)
COMMENT = 'Country business groupings'
AS 
SELECT * FROM snowflake_sample_data.tpch_sf10.region
WHERE R_REGIONKEY < 4
;


-- ************************************** location
CREATE OR REPLACE  TABLE location
(
 location_id number(38,0) NOT NULL,
 name        varchar(25) NOT NULL,
 region_id   number(38,0) NOT NULL,
 comment     varchar(152) COMMENT 'varchar(152) COMMENT ''VARCHAR(152)',
 CONSTRAINT pk_location PRIMARY KEY ( location_id) RELY,
 CONSTRAINT fk_location FOREIGN KEY ( region_id) REFERENCES region RELY,
 CONSTRAINT ak_location_name UNIQUE ( name ) RELY
)
COMMENT = 'location assigned to 
customer or supplier'
AS 
SELECT n_nationkey, n_name, n_regionkey, n_comment  
FROM snowflake_sample_data.tpch_sf10.nation  
;



-- demonstrate join elimination without FK constraints
SELECT 'left join no filter' as method , count(*) cnt FROM location 
left join region_main  rm using(region_id);


-- run a query demonstrating proper inner join behavior with join elimination
SELECT 'inner join' as method ,count(*) cnt FROM location 
inner join region_main  rm using(region_id)
union all 
SELECT 'left join no filter' as method , count(*) cnt FROM location 
left join region_main  rm using(region_id)
union all 
SELECT 'left join filter' as method , count(*) cnt FROM location 
left join region_main  rm using(region_id)
WHERE TRUE 
and rm.region_id is not null 
--avoid cache
and year(current_date)=2023
;





-- create an incorrect FK reference (to a table with a subset of FK values)
CREATE or replace   TABLE location_bad_fk
(
 location_id number(38,0) NOT NULL,
 name        varchar(25) NOT NULL,
 region_id   number(38,0) NOT NULL,
 comment     varchar(152) COMMENT 'varchar(152) COMMENT ''VARCHAR(152)',
 CONSTRAINT pk_location PRIMARY KEY ( location_id) rely ,
 CONSTRAINT ak_location_name UNIQUE ( name ) rely ,
 CONSTRAINT fk_location_in_region_main foreign key ( region_id) references region_main ( region_id ) rely
)
COMMENT = 'location assigned to 
customer or supplier'
AS 
SELECT n_nationkey, n_name, n_regionkey, n_comment  
FROM snowflake_sample_data.tpch_sf10.nation  
;



-- run a query demonstrating incorrect inner join due to join elimination
SELECT 'inner join' as method ,count(*) cnt FROM location_bad_fk 
inner join region_main  rm using(region_id)
union all 
SELECT 'left join no filter' as method , count(*) cnt FROM location_bad_fk 
left join region_main  rm using(region_id)
union all 
SELECT 'left join filter' as method , count(*) cnt FROM location_bad_fk 
left join region_main  rm using(region_id)
WHERE TRUE 
and rm.region_id is not null 
--avoid cache
and year(current_date)=2023
;



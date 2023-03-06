/*
 * 
 * Working with semi-structured data
 * 
 * 
 */

---------------------------------------------------------------------------------------------------------------------
-- Create a table and load semi-structured data
---------------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE SCHEMA ch15_semistruct;


CREATE OR REPLACE TABLE pirate_json
(
 __load_id   number NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
 __load_name varchar NOT NULL,
 __load_dts  timestamp_ntz NOT NULL,
 v           variant NOT NULL,

 CONSTRAINT pirate_json___load_id PRIMARY KEY ( __load_id )
)
COMMENT = 'table w. a variant for pirate data, with meta ELT fields';

INSERT INTO pirate_json (-- __load_id is omitted to allow autoincrement
  __load_name
, __load_dts
, v
) 
SELECT 'ad-hoc load', current_timestamp, PARSE_JSON($1) FROM 
VALUES ('
{
  "name": "Edward Teach",
  "nickname": "Blackbeard",
  "years_active": [
    1716,
    1717,
    1718
  ],
  "born": 1680,
  "died": 1718,
  "cause_of_death": "Killed in action",
  "crew": [
    {
      "name": "Stede Bonnet",
      "nickname": "Gentleman pirate",
      "weapons": [
        "blunderbuss"
      ],
      "years_active": [
        1717,
        1718
      ]
    },
    {
      "name": "Israel Hands",
      "nickname": null,
      "had_bird": true,
      "weapons": [
        "flintlock pistol",
        "cutlass",
        "boarding axe"
      ],
      "years_active": [
        1716,
        1717,
        1718
      ]
    }
  ],
  "ship": {
    "name": "Queen Anne\'s Revenge",
    "type": "Frigate",
    "original_name": "La Concorde",
    "year_captured": 1717
  }
}
')
;


---------------------------------------------------------------------------------------------------------------------
-- Read from semi-structured data
---------------------------------------------------------------------------------------------------------------------

SELECT * FROM pirate_json;
            
            
--cast and alias basic attributes
SELECT v:name AS pirate_name_json
 , v:name::STRING AS pirate_name_string
 , v:nickname::STRING AS pirate_name_string
FROM pirate_json; 


--select sub-columns
SELECT v:name::STRING AS pirate_name
, v:ship.name::STRING AS ship_name
FROM pirate_json; 


--select a colum that has vanished
SELECT v:name::STRING AS pirate_name
, v:loc_buried_treasure::STRING AS pirate_treasure_location
FROM pirate_json; 


--select from an array
SELECT v:name::STRING AS pirate_name
, v:years_active AS years_active
, v:years_active[0] AS active_from
, v:years_active[ARRAY_SIZE(v:years_active)-1] AS active_to
FROM pirate_json; 


--query multiple elements
SELECT v:name::STRING AS pirate_name
 , v:crew::VARIANT AS pirate_crew
FROM pirate_json; 


--flatten crew members into rows
SELECT v:name::STRING AS pirate_name
, c.VALUE:name::STRING AS crew_name
, c.VALUE:nickname::STRING AS crew_nickname
FROM pirate_json, LATERAL FLATTEN(v:crew) c; 


--flatten crew members and their weapons into rows
SELECT v:name::STRING AS pirate_name
, c.VALUE:name::STRING AS crew_name
, w.value::STRING AS crew_weapons
FROM pirate_json, LATERAL FLATTEN(v:crew) c
		,LATERAL flatten(c.value:weapons) w;

--perform aggregation and filtering just like on tabular data 
SELECT COUNT(crew_weapons) AS num_weapons FROM (
SELECT c.VALUE:name::STRING AS crew_name
, w.value::STRING AS crew_weapons
FROM pirate_json, LATERAL FLATTEN(v:crew) c
		,LATERAL flatten(c.value:weapons) w
WHERE crew_name = 'Israel Hands' );



---------------------------------------------------------------------------------------------------------------------
-- Determine the depth and levels of semi-structured data
---------------------------------------------------------------------------------------------------------------------

--recursive flatten
SELECT f.*
FROM pirate_json p, LATERAL FLATTEN( v , RECURSIVE => TRUE ) f
WHERE TRUE 
;



--get the object attributes and their depth    
SELECT ARRAY_SIZE(
		STRTOK_TO_ARRAY (IFF ( 
					STARTSWITH( RIGHT(path,3), '['), 
	    			LEFT(PATH ,LENGTH(path)-3) || '.'|| SUBSTR(path,LENGTH(PATH)-1,2), 
	    			path), '.' 
	    			) 
	   ) AS depth
, f.key
, f.path
, f.value
FROM pirate_json p, LATERAL FLATTEN( v , RECURSIVE => TRUE ) f
ORDER BY 1 ASC ;


---------------------------------------------------------------------------------------------------------------------
-- Create and load the relational schema	
---------------------------------------------------------------------------------------------------------------------

--create weapons table
CREATE OR REPLACE TABLE weapon
(
 weapon_id   number(38,0) NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
 name        varchar NOT NULL,
 __load_name varchar NOT NULL,
 __load_dts  timestamp_ntz NOT NULL,

 CONSTRAINT pk_weapon_weapon_id PRIMARY KEY ( weapon_id ),
 CONSTRAINT ak_weapon_name UNIQUE ( name )
)
COMMENT = 'weapons used by pirates'; 



--insert weapons from current load and generate surrogate keys
MERGE INTO weapon w
USING ( SELECT  w.value::STRING AS weapon_name
,  __load_dts
FROM pirate_json, LATERAL FLATTEN(v:crew) c
		,LATERAL flatten(c.value:weapons) w
WHERE TRUE 
AND __load_dts = (SELECT max(__load_dts) FROM pirate_json)) s
ON w.name = s.weapon_name
WHEN NOT MATCHED THEN INSERT ( --weapon_id will be auto-generated from a sequence 
name,
 __load_name,
 __load_dts
)
VALUES(
weapon_name,
'ad-hoc load', 
  __load_dts
)
;


SELECT weapon_id, name FROM weapon;


--create ship table
CREATE OR REPLACE TABLE ship
(
 ship_id       integer NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
 name          varchar NOT NULL,
 type          varchar,
 original_name varchar,
 year_captured number(38,0),
 __load_name   varchar NOT NULL,
 __load_dts    timestamp_ntz NOT NULL,

 CONSTRAINT pk_ship_ship_id PRIMARY KEY ( ship_id ),
 CONSTRAINT ak_ship_name UNIQUE ( name )
);






--insert ships from current load and generate surrogate keys
MERGE INTO ship 
USING ( SELECT  v:ship.name::STRING AS ship_name
, v:ship.type::STRING AS ship_type
, v:ship.original_name::STRING AS ship_original_name
, v:ship.year_captured::STRING AS ship_year_captured
, __load_dts
FROM pirate_json
WHERE TRUE 
AND __load_dts = (SELECT max(__load_dts) FROM pirate_json)) s
ON ship.name = s.ship_name
WHEN NOT MATCHED THEN INSERT ( --ship_id will be auto-generated from a sequence 
name,
type,
original_name,
year_captured,
__load_name,
__load_dts
)
VALUES(
ship_name,
ship_type,
ship_original_name,
ship_year_captured,
'ad-hoc load', 
__load_dts
)
;


--create pirate table
CREATE OR REPLACE TABLE pirate
(
 pirate_id      integer NOT NULL AUTOINCREMENT START 1 INCREMENT 1,
 ship_id        integer NOT NULL,
 crew_of        integer ,
 name           varchar NOT NULL,
 nickname       varchar,
 had_parrot     boolean,
 year_born      number(38,0),
 year_died      number(38,0),
 cause_of_death varchar,
 __load_name    varchar NOT NULL,
 __load_dts     timestamp_ntz NOT NULL,

 CONSTRAINT pk_pirate_pirate_id PRIMARY KEY ( pirate_id ),
 CONSTRAINT ak_pirate_name UNIQUE ( name ),
 CONSTRAINT FK_54 FOREIGN KEY ( ship_id ) REFERENCES ch15.ship ( ship_id ),
 CONSTRAINT fk_pirate_crew_reports_to FOREIGN KEY ( crew_of ) REFERENCES ch15.pirate ( pirate_id )
);



--insert top level pirates from current load
MERGE INTO pirate p
USING ( SELECT ship.ship_id AS ship_id
, NULL::integer AS crew_of
, v:name::STRING AS name
, v:nickname::STRING AS nickname
, v:born::STRING AS year_born
, v:died::STRING AS year_died
, v:cause_of_death::STRING AS cause_of_death
, p.__load_dts
FROM pirate_json p
INNER JOIN ship ON v:ship.name::STRING = ship.NAME
WHERE TRUE 
AND p.__load_dts = (SELECT max(__load_dts) FROM pirate_json)) s
ON p.name = s.name
WHEN NOT MATCHED THEN INSERT ( --pirate_id will be auto-generated from a sequence    
ship_id       
,crew_of       
,name          
,nickname      
,had_parrot    
,year_born     
,year_died     
,cause_of_death
,__load_name   
,__load_dts    
)
VALUES(
   s.ship_id       
 , null      
 , s.name          
 , s.nickname      
 , null    
 , s.year_born     
 , s.year_died     
 , s.cause_of_death
 , 'ad-hoc load'      
 , s.__load_dts        
)
;




--insert crew members from current load
MERGE INTO pirate p
USING ( SELECT ship.ship_id AS ship_id
,  pc.pirate_id AS crew_of
, c.value:name::STRING AS name
, c.value:nickname::STRING AS nickname
, c.value:had_bird::boolean AS had_bird
, p.__load_dts
FROM pirate_json p
INNER JOIN ship ON v:ship.name::STRING = ship.NAME
INNER JOIN pirate pc ON pc.name = v:name::STRING
, LATERAL FLATTEN(v:crew) c
WHERE TRUE 
AND p.__load_dts = (SELECT max(__load_dts) FROM pirate_json)) s
ON p.name = s.name
WHEN NOT MATCHED THEN INSERT ( --pirate_id will be auto-generated from a sequence    
ship_id       
,crew_of       
,name          
,nickname      
,had_parrot    
,year_born     
,year_died     
,cause_of_death
,__load_name   
,__load_dts    
)
VALUES(
   s.ship_id       
 , s.crew_of      
 , s.name          
 , s.nickname      
 , s.had_bird    
 , null     
 , null     
 , null
 , 'ad-hoc load'      
 , s.__load_dts        
)
;

SELECT * FROM pirate;


--create pirate active years table 
CREATE OR REPLACE TABLE pirate_years_active
(
 pirate_id   integer NOT NULL,
 year_active number(38,0) NOT NULL,
 __load_name varchar NOT NULL,
 __load_dts  timestamp_ntz NOT NULL,

 CONSTRAINT pk_pirate_years_active PRIMARY KEY ( pirate_id, year_active ),
 CONSTRAINT FK_58 FOREIGN KEY ( pirate_id ) REFERENCES pirate ( pirate_id )
);





--insert top level pirates from current load
MERGE INTO pirate_years_active pya
USING ( SELECT  pc.pirate_id AS pirate_id
, y.VALUE::INTEGER AS year_active
, p.__load_name
, p.__load_dts
FROM pirate_json p
INNER JOIN pirate pc ON pc.name = v:name::STRING
, LATERAL FLATTEN(v:years_active) y
WHERE TRUE 
AND p.__load_dts = (SELECT max(__load_dts) FROM pirate_json)) s
ON pya.pirate_id = s.pirate_id
AND pya.year_active = s.year_active
WHEN NOT MATCHED THEN INSERT ( 
pirate_id
, year_active
, __load_name   
, __load_dts    
)
VALUES(
pirate_id
, year_active
, __load_name   
, __load_dts      
)
;


--insert crew members from current load
MERGE INTO pirate_years_active pya
	USING ( SELECT pc.pirate_id AS pirate_id, crew.* FROM (
	SELECT  c.value:name::STRING AS pirate_name
	, y.VALUE::INTEGER AS year_active
	, p.__load_name
	, p.__load_dts
	FROM pirate_json p
	, LATERAL FLATTEN(v:crew) c
	,LATERAL flatten(c.value:years_active) y
	WHERE TRUE 
	AND p.__load_dts = (SELECT max(__load_dts) FROM pirate_json)
	) crew 
	INNER JOIN pirate pc ON pc.name = crew.pirate_name) s
ON pya.pirate_id = s.pirate_id
AND pya.year_active = s.year_active
WHEN NOT MATCHED THEN INSERT ( 
pirate_id
, year_active
, __load_name   
, __load_dts    
)
VALUES(
pirate_id
, year_active
, __load_name   
, __load_dts      
)
;

SELECT * FROM pirate_years_active ;


--create pirate weapons associative table 
CREATE OR REPLACE TABLE pirate_weapons
(
 pirate_id   integer NOT NULL,
 weapon_id   number(38,0) NOT NULL,
 __load_name varchar NOT NULL,
 __load_dts  timestamp_ntz NOT NULL,

 CONSTRAINT pk_pirate_weapon PRIMARY KEY ( pirate_id, weapon_id ),
 CONSTRAINT fk_pirate_weapon_pirate FOREIGN KEY ( pirate_id ) REFERENCES pirate ( pirate_id ),
 CONSTRAINT fk_pirate_weapon_weapon FOREIGN KEY ( weapon_id ) REFERENCES weapon ( weapon_id )
);



--insert pirate weapons from current load
MERGE INTO pirate_weapons pw
	USING ( SELECT pc.pirate_id AS pirate_id, wc.weapon_id, crew.* FROM (
	SELECT  c.VALUE:name::STRING AS crew_name
	, w.value::STRING AS crew_weapon_name
	, p.__load_name
	, p.__load_dts
	FROM pirate_json p, LATERAL FLATTEN(v:crew) c
			,LATERAL flatten(c.value:weapons) w
	WHERE TRUE 
		AND p.__load_dts = (SELECT max(__load_dts) FROM pirate_json)
	) crew 
	INNER JOIN pirate pc ON pc.name = crew.crew_name
	INNER JOIN weapon wc ON wc.name = crew.crew_weapon_name) s
ON pw.pirate_id = s.pirate_id
AND pw.weapon_id = s.weapon_id
WHEN NOT MATCHED THEN INSERT ( 
pirate_id
, weapon_id
, __load_name   
, __load_dts    
)
VALUES(
pirate_id
, weapon_id
, __load_name   
, __load_dts      
)
;		


--A sample relational query over the final schema
SELECT p.NAME AS pirate_name
, NVL(p.nickname, 'none') AS nickname
, s.type AS ship_type
, NVL(w.NAME , 'none') AS  weapon_name
FROM pirate  p
INNER JOIN ship  s USING (ship_id)
LEFT JOIN pirate_weapons pw USING (pirate_id)
LEFT JOIN weapon w USING (weapon_id) 
;



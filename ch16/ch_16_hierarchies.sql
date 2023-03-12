/*
 * 
 * Working with hierarchies
 * 
 * 
 */
 
---------------------------------------------------------------------------------------------------------------------
-- Create the base table and load data 
--------------------------------------------------------------------------------------------------------------------- 
 
CREATE OR REPLACE SCHEMA ch16_hier;

CREATE OR REPLACE TABLE pirate (
	pirate_id NUMBER(38,0) NOT NULL,
	name VARCHAR(50) NOT NULL,
	rank VARCHAR(50) NOT NULL,
	superior_id NUMBER(38,0),

	PRIMARY KEY (pirate_id),
	FOREIGN KEY (superior_id) REFERENCES pirate(pirate_id)
);



INSERT INTO
    pirate (pirate_id, name, rank, superior_id)
VALUES
    (1, 'Blackbeard', 'Captain', NULL),
    (2, 'Calico Jack', 'First Mate', 1),
    (3, 'Anne Bonny', 'Second Mate', 1),
    (4, 'Mary Read', 'Navigator', 2),
    (5, 'Israel Hands', 'Boatswain', 2),
    (6, 'John Silver', 'Carpenter', 3),
    (7, 'Long John', 'Gunner', 3),
    (8, 'Billy Bones', 'Cook', 4),
    (9, 'Tom Morgan', 'Sailor', 5),
    (10, 'Harry Hawkins', 'Sailor', 5),
    (11, 'Black Dog', 'Sailor', 6),
    (12, 'Dick Johnson', 'Sailor', 6),
    (13, 'Roger Pew', 'Sailor', 7),
    (14, 'Dirk van der Heide', 'Sailor', 7),
    (15, 'Ned Low', 'Sailor', 9),
    (16, 'Edward England', 'Sailor', 9),
    (17, 'Stede Bonnet', 'Sailor', 10),
    (18, 'Charles Vane', 'Sailor', 10),
    (19, 'James Kidd', 'Sailor', 11),
    (20, 'William Kidd', 'Sailor', 11)
;

SELECT * FROM pirate;


---------------------------------------------------------------------------------------------------------------------
-- Determine the depth and hierarchy path using CONNECT BY
---------------------------------------------------------------------------------------------------------------------

SELECT
	  name
	, pirate_id
	, superior_id
	, rank
	, sys_connect_by_path(rank, ' -> ') AS PATH
	--LEVEL is a pesuedo-column returned by CONNECT BY 
	--which indicates the current level of the hierarchy
	, level
FROM	pirate
START WITH	rank = 'Captain'
CONNECT BY 	superior_id = PRIOR pirate_id
ORDER BY level
;


---------------------------------------------------------------------------------------------------------------------
-- Separate the hierarchy into multiple root branches
---------------------------------------------------------------------------------------------------------------------
SELECT
	  name
	, pirate_id
	, superior_id
	, rank
	, sys_connect_by_path(rank, ' -> ') AS PATH
	, CONNECT_BY_ROOT rank AS crew_branch
	, level + 1 AS level
FROM	pirate
--parse the hierarchy tree starting at the First and Second Mates as roots
START WITH	STRTOK(rank, ' ', 2) = 'Mate'
CONNECT BY 	superior_id = PRIOR pirate_id
/* "If I did not now and then kill one of them, 
*  they would forget who I was" - Blackbeard
*  
*  Don't forget the Captain...
*/
UNION ALL 
SELECT
    name,
    pirate_id,
    superior_id,
    rank,
    rank  AS path,
    rank  AS crew_branch,
    1 AS level
FROM 
    pirate
    WHERE TRUE  
    and rank = 'Captain'
ORDER BY  crew_branch, level
;


---------------------------------------------------------------------------------------------------------------------
-- Calculate and flag edge nodes
---------------------------------------------------------------------------------------------------------------------
with hier as (SELECT
	  name
	, pirate_id
	, superior_id
	, rank
	, sys_connect_by_path(rank, ' -> ') AS PATH
	, CONNECT_BY_ROOT rank AS crew_branch
	, level + 1 AS level
FROM	pirate
START WITH	STRTOK(rank, ' ', 2) = 'Mate'
CONNECT BY 	superior_id = PRIOR pirate_id
UNION ALL 
SELECT
    name,
    pirate_id,
    superior_id,
    rank,
    rank  AS path,
    rank  AS crew_branch,
    1 AS level
FROM 
    pirate
    WHERE TRUE  
    AND rank = 'Captain'
ORDER BY  crew_branch, level
)
--by joining to a distinct list of superiors
, super AS (
SELECT  DISTINCT  superior_id FROM  hier
)
SELECT  h1.* 
--null values can be flagged as edges
, IFF(s.superior_id IS NULL, true, false) is_edge_node 
FROM hier h1 
LEFT JOIN super s ON h1.pirate_id = s.superior_id
;

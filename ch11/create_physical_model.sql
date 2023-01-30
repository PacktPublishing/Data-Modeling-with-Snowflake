create database adventureworks; 

create schema operations 
WITH MANAGED ACCESS  
DATA_RETENTION_TIME_IN_DAYS = 14;



-- ************************************** part
CREATE  TABLE part
(
 part_id          number(38,0) NOT NULL,
 name             varchar NOT NULL,
 manufacturer     varchar NOT NULL,
 brand            varchar NOT NULL,
 type             varchar NOT NULL,
 size_centimeters number(38,0) NOT NULL,
 container        varchar NOT NULL,
 retail_price_usd number(12,2) NOT NULL,
 comment          varchar COMMENT 'varchar COMMENT ''VARCHAR(23)',

 CONSTRAINT pk_part PRIMARY KEY ( part_id ) RELY
)
COMMENT = 'Parts we distribute';

-- ************************************** location
CREATE  TABLE location
(
 location_id number(38,0) NOT NULL,
 name        varchar(25) NOT NULL,
 region_id   number(38,0) NOT NULL,
 comment     varchar(152) COMMENT 'varchar(152) COMMENT ''VARCHAR(152)',
 CONSTRAINT pk_location PRIMARY KEY ( location_id) RELY,
 CONSTRAINT ak_location_name UNIQUE ( name ) RELY
)
COMMENT = 'location assigned to 
customer or supplier';

-- ************************************** supplier
CREATE  TABLE supplier
(
 supplier_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 comment             varchar COMMENT 'varchar COMMENT ''VARCHAR(101)',
 CONSTRAINT pk_supplier PRIMARY KEY ( supplier_id ) RELY,
 CONSTRAINT FK_SUPPLIER_BASED_IN_LOCATION FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) RELY
)
COMMENT = 'Suppliers who we buy from';

-- ************************************** customer
CREATE  TABLE customer
(
 customer_id         number(38,0) NOT NULL,
 name                varchar NOT NULL,
 address             varchar NOT NULL,
 location_id         number(38,0) NOT NULL,
 phone               varchar(15) NOT NULL,
 account_balance_usd number(12,2) NOT NULL,
 market_segment      varchar(10) NOT NULL,
 comment             varchar COMMENT 'varchar COMMENT ''VARCHAR(117)',

 CONSTRAINT pk_customer PRIMARY KEY ( customer_id ) RELY,
 CONSTRAINT FK_CUSTOMER_BASED_IN_LOCATION FOREIGN KEY ( location_id ) REFERENCES location ( location_id ) RELY
)
COMMENT = 'Registered cusotmers';

-- ************************************** sales_order
CREATE  TABLE sales_order
(
 sales_order_id  number(38,0) NOT NULL,
 customer_id     number(38,0) NOT NULL,
 order_status    varchar(1),
 total_price_usd number(12,2),
 order_date      date,
 order_priority  varchar(15),
 clerk           varchar(15),
 ship_priority   number(38,0),
 comment         varchar(79) COMMENT 'varchar(79) COMMENT ''VARCHAR(79)',

 CONSTRAINT pk_sales_order PRIMARY KEY ( sales_order_id ) RELY,
 CONSTRAINT FK_SALES_ORDER_PLACED_BY_CUSTOMER FOREIGN KEY ( customer_id ) REFERENCES customer ( customer_id ) RELY
)
COMMENT = 'single order per customer';



-- ************************************** inventory
CREATE  TABLE inventory
(
 part_id           number(38,0) NOT NULL COMMENT 'part of unique identifier with ps_suppkey',
 supplier_id       number(38,0) NOT NULL COMMENT 'part of unique identifier with ps_partkey',
 available_amount  number(38,0) NOT NULL COMMENT 'number of parts available for sale',
 supplier_cost_usd number(12,2) NOT NULL COMMENT 'original cost paid to supplier',
 comment           varchar() COMMENT 'varchar(79) COMMENT ''VARCHAR(79)',

 CONSTRAINT pk_inventory PRIMARY KEY ( part_id, supplier_id ) RELY,
 CONSTRAINT FK_INVENTORY_STORES_PART FOREIGN KEY ( part_id ) REFERENCES part ( part_id ) RELY,
 CONSTRAINT FK_INVENTORY_SUPPLIED_BY_SUPPLIER FOREIGN KEY ( supplier_id ) REFERENCES supplier ( supplier_id ) RELY
)
COMMENT = 'Warehouse Inventory';

-- ************************************** lineitem
CREATE  TABLE lineitem
(
 line_number        number(38,0) NOT NULL,
 sales_order_id     number(38,0) NOT NULL,
 part_id            number(38,0) NOT NULL,
 supplier_id        number(38,0) NOT NULL,
 quantity           number(12,2),
 extended_price_usd number(12,2),
 discount_percent   number(12,2),
 tax_percent        number(12,2),
 return_flag        varchar(1),
 line_status        varchar(1),
 ship_date          date,
 commit_date        date,
 receipt_date       date,
 ship_instructions  varchar(25),
 ship_mode          varchar(10),
 comment            varchar(44) COMMENT 'varchar(44) COMMENT ''VARCHAR(44)',

 CONSTRAINT pk_lineitem PRIMARY KEY ( line_number, sales_order_id ) RELY,
 CONSTRAINT FK_LINEITEM_CONSISTS_OF_SALES_ORDER FOREIGN KEY ( sales_order_id ) REFERENCES sales_order ( sales_order_id ) RELY,
 CONSTRAINT FK_LINEITEM_CONTAINING_PART FOREIGN KEY ( part_id ) REFERENCES part ( part_id ) RELY,
 CONSTRAINT FK_LINEITEM_SUPPLIED_BY_SUPPLIER FOREIGN KEY ( supplier_id ) REFERENCES supplier ( supplier_id ) RELY
)
COMMENT = 'various line items per order';


-- ************************************** loyalty_customer
CREATE  TABLE loyalty_customer
(
 customer_id   number(38,0) NOT NULL,
 level         varchar NOT NULL COMMENT 'customer full name',
 type          varchar NOT NULL COMMENT 'loyalty tier: bronze, silver, or gold',
 points_amount number NOT NULL,
 comment       varchar COMMENT 'customer loyalty status calculated from sales order volume',

 CONSTRAINT pk_loyalty_customer PRIMARY KEY ( customer_id ) RELY,
 CONSTRAINT "1" FOREIGN KEY ( customer_id ) REFERENCES customer ( customer_id ) RELY
)
COMMENT = 'client loyalty program with gold, silver, bronze status';

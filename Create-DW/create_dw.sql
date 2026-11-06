-- =============================================================================
-- create_dw.sql
-- Retail chain Data Warehouse: star schema plus the ETL working tables.
--
-- Course   : DS3003 & DS3004, Data Warehousing and Business Intelligence
-- Student  : Shahoud Shahid, 23i-2515, DS-A
-- Target   : MySQL 8.0, InnoDB, utf8mb4
--
-- How to run
--   The database is chosen by the caller, so this file has no USE and no
--   CREATE DATABASE. The Python program runs it at startup against the database
--   name the user typed. By hand:
--       mysql -h <host> -P <port> -u <user> -p <database> < create_dw.sql
--   It is safe to run again: every run first removes the old copy.
--
-- Layout of this file
--   Section 1  Drop every old object, in dependency order
--   Section 2  Dimension tables
--   Section 3  Fact table, with the grain of the warehouse
--   Section 4  Working tables used by the ETL, which are not part of the star
--
-- The dimensional model
--
--                          dim_customer
--                               |
--          dim_product ---- fact_sales ---- dim_supplier
--                             /    \
--                     dim_store    dim_time
--
--   This is a star: one fact table in the centre and one join from the fact to
--   each of the five dimensions. Dimensions are not joined to each other, so
--   supplier has its own key on the fact and is not reached through product.
--   A query can group by any attribute after a single join.
--
--   Every dimension uses a surrogate key created here, and keeps the original
--   id from the source as a UNIQUE natural key. The loader uses that natural key
--   to find out if a dimension value already exists before it inserts the fact.
--
--   All id columns use utf8mb4_bin, so ids are compared exactly. The raw file has
--   112 product codes that differ only by letter case, such as 85123A and 85123a,
--   and a case-insensitive collation would treat each such pair as one id. None of
--   these pairs survive cleaning in this file, but the schema does not rely on that.
-- =============================================================================


-- =============================================================================
-- Section 1: remove any old copy
--
-- Order matters. The view reads the fact and the dimensions, so it goes first.
-- The fact holds the foreign keys, so it goes before the dimensions it points
-- to. Working tables have no foreign keys and can go in any order.
-- =============================================================================

DROP VIEW IF EXISTS STORE_QUARTERLY_SALES;

DROP TABLE IF EXISTS fact_sales;

DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_time;

DROP TABLE IF EXISTS customer_master_data;
DROP TABLE IF EXISTS product_master_data;
DROP TABLE IF EXISTS periodic_sales_data;
DROP TABLE IF EXISTS shed_table;
DROP TABLE IF EXISTS live_cache;
DROP TABLE IF EXISTS etl_hourly_log;
DROP TABLE IF EXISTS etl_reject;
DROP TABLE IF EXISTS etl_progress;
DROP TABLE IF EXISTS etl_live_status;


-- =============================================================================
-- Section 2: dimension tables
--
-- Rows are added by the loader the first time a value appears, before the fact
-- row that needs it (PDF section 7). The CHECK constraints repeat the fixed value
-- lists from PDF section 5.4, so a bug in the rules fails at load time and does
-- not end up as a wrong group in a report.
-- =============================================================================

-- Who bought. Attributes come from the customer Master Data. Years in the city
-- is text because one of its values is 4+.
CREATE TABLE dim_customer (
    customer_key               INT UNSIGNED NOT NULL AUTO_INCREMENT,
    customer_id                VARCHAR(12)  COLLATE utf8mb4_bin NOT NULL,
    gender                     VARCHAR(6)   NOT NULL,
    age_group                  VARCHAR(5)   NOT NULL,
    occupation                 VARCHAR(12)  NOT NULL,
    city_category              CHAR(1)      NOT NULL,
    stay_in_current_city_years VARCHAR(2)   NOT NULL,
    marital_status             VARCHAR(7)   NOT NULL,
    PRIMARY KEY (customer_key),
    CONSTRAINT uq_dim_customer_id UNIQUE (customer_id),
    CONSTRAINT chk_dim_customer_gender CHECK (gender IN ('Male', 'Female')),
    CONSTRAINT chk_dim_customer_age CHECK (age_group IN
        ('0-17', '18-25', '26-35', '36-45', '46-50', '51-55', '55+')),
    CONSTRAINT chk_dim_customer_occupation CHECK (occupation IN
        ('Student', 'Engineer', 'Teacher', 'Executive', 'Clerk',
         'Healthcare', 'Retail', 'Homemaker', 'Artist', 'Other')),
    CONSTRAINT chk_dim_customer_city CHECK (city_category IN ('A', 'B', 'C')),
    CONSTRAINT chk_dim_customer_stay CHECK (stay_in_current_city_years IN
        ('0', '1', '2', '3', '4+')),
    CONSTRAINT chk_dim_customer_marital CHECK (marital_status IN ('Single', 'Married'))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Customer dimension, one row per customer id';

-- What was sold. Attributes come from the product Master Data.
CREATE TABLE dim_product (
    product_key      INT UNSIGNED NOT NULL AUTO_INCREMENT,
    product_id       VARCHAR(20)  COLLATE utf8mb4_bin NOT NULL,
    product_name     VARCHAR(255) NOT NULL,
    product_category VARCHAR(20)  NOT NULL,
    PRIMARY KEY (product_key),
    CONSTRAINT uq_dim_product_id UNIQUE (product_id),
    CONSTRAINT chk_dim_product_category CHECK (product_category IN
        ('Home Decor', 'Kitchen', 'Stationery', 'Toys', 'Seasonal Gifts',
         'Bags', 'Jewelry', 'Garden', 'Party', 'Other'))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Product dimension, one row per product id';

-- Who supplied the product. Kept as its own dimension so the fact joins to it
-- directly, which keeps the schema a star and not a snowflake.
CREATE TABLE dim_supplier (
    supplier_key  SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    supplier_id   VARCHAR(8)  COLLATE utf8mb4_bin NOT NULL,
    supplier_name VARCHAR(40) NOT NULL,
    PRIMARY KEY (supplier_key),
    CONSTRAINT uq_dim_supplier_id UNIQUE (supplier_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Supplier dimension, one row per supplier id';

-- Where it was sold. Five stores only (PDF section 5.4.2): store 1 is the
-- website and stores 2 to 5 are the physical shops.
CREATE TABLE dim_store (
    store_key  SMALLINT UNSIGNED NOT NULL AUTO_INCREMENT,
    store_id   TINYINT UNSIGNED  NOT NULL,
    store_name VARCHAR(40) NOT NULL,
    city       VARCHAR(30) NOT NULL,
    PRIMARY KEY (store_key),
    CONSTRAINT uq_dim_store_id UNIQUE (store_id),
    CONSTRAINT chk_dim_store_id CHECK (store_id BETWEEN 1 AND 5)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Store dimension, one row per store id';

-- When it was sold, at one minute resolution because that is the resolution of
-- the source event_time. The key is built from the value as YYYYMMDDHHMM and is
-- not AUTO_INCREMENT, so the same minute always gets the same key. The parts are
-- stored as columns so a query can group by them without reading the timestamp.
--   day_of_week: 1 = Monday through 7 = Sunday
--   day_type   : Weekday is Monday to Friday, Weekend is Saturday and Sunday
--   season     : Spring Mar-May, Summer Jun-Aug, Fall Sep-Nov, Winter Dec-Feb
--   half_year  : H1 is January to June, H2 is July to December
CREATE TABLE dim_time (
    time_key      BIGINT UNSIGNED NOT NULL COMMENT 'YYYYMMDDHHMM',
    full_datetime DATETIME   NOT NULL,
    full_date     DATE       NOT NULL,
    hour_of_day   TINYINT    NOT NULL,
    day_of_month  TINYINT    NOT NULL,
    month_no      TINYINT    NOT NULL,
    month_name    VARCHAR(9) NOT NULL,
    quarter_no    TINYINT    NOT NULL,
    year_no       SMALLINT   NOT NULL,
    day_of_week   TINYINT    NOT NULL COMMENT '1=Monday .. 7=Sunday',
    day_name      VARCHAR(9) NOT NULL,
    day_type      VARCHAR(7) NOT NULL,
    season        VARCHAR(6) NOT NULL,
    half_year     CHAR(2)    NOT NULL,
    PRIMARY KEY (time_key),
    CONSTRAINT uq_dim_time_datetime UNIQUE (full_datetime),
    INDEX idx_dim_time_year_month (year_no, month_no),
    INDEX idx_dim_time_date (full_date),
    CONSTRAINT chk_dim_time_hour CHECK (hour_of_day BETWEEN 0 AND 23),
    CONSTRAINT chk_dim_time_day CHECK (day_of_month BETWEEN 1 AND 31),
    CONSTRAINT chk_dim_time_month CHECK (month_no BETWEEN 1 AND 12),
    CONSTRAINT chk_dim_time_quarter CHECK (quarter_no BETWEEN 1 AND 4),
    CONSTRAINT chk_dim_time_weekday CHECK (day_of_week BETWEEN 1 AND 7),
    CONSTRAINT chk_dim_time_day_type CHECK (day_type IN ('Weekday', 'Weekend')),
    CONSTRAINT chk_dim_time_season CHECK (season IN ('Spring', 'Summer', 'Fall', 'Winter')),
    CONSTRAINT chk_dim_time_half CHECK (half_year IN ('H1', 'H2'))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Time dimension, one row per minute that has a sale';


-- =============================================================================
-- Section 3: fact table
--
-- GRAIN: one row is one transaction, meaning one product in one shopping cart,
-- with one customer, one store, one supplier and one time. Carts are not rolled
-- up into a coarser row, because Questions 16 and 19 need the separate
-- transactions. Business totals are computed in SQL and are never stored as a
-- second fact table (PDF section 6.2).
--
-- Exact duplicate rows in the source are kept, so there is deliberately no
-- unique constraint on the business columns.
--
-- Measures
--   quantity : copied from the transaction
--   revenue  : SALE = quantity x unit price, computed once during consolidation.
--              It is in neither the transactional data nor the Master Data, and
--              the SQL questions must use this stored value.
--
-- Degenerate dimension
--   transaction_id is the shopping cart. It has no attributes of its own so it
--   stays on the fact. The index on (transaction_id, product_key) serves the
--   self join in Question 16, products bought in the same cart.
--
-- Lineage
--   sales_mode      : how the sale happened, Online or Periodic (the MD5 rule)
--   processing_mode : how the row reached the warehouse, one of
--                     Live     through the near real time path
--                     Periodic through the hourly extraction
--                     Shed     shed to disk in a peak hour and processed later
--
-- InnoDB creates an index for each foreign key below, so joins from the fact to
-- any dimension are indexed.
-- =============================================================================

CREATE TABLE fact_sales (
    sales_key       BIGINT UNSIGNED   NOT NULL AUTO_INCREMENT,
    customer_key    INT UNSIGNED      NOT NULL,
    product_key     INT UNSIGNED      NOT NULL,
    supplier_key    SMALLINT UNSIGNED NOT NULL,
    store_key       SMALLINT UNSIGNED NOT NULL,
    time_key        BIGINT UNSIGNED   NOT NULL,
    transaction_id  VARCHAR(10) COLLATE utf8mb4_bin NOT NULL,
    sales_mode      VARCHAR(8)    NOT NULL,
    processing_mode VARCHAR(8)    NOT NULL,
    quantity        INT           NOT NULL,
    revenue         DECIMAL(14,3) NOT NULL,
    loaded_at       DATETIME(3)   NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (sales_key),
    INDEX idx_fact_cart_product (transaction_id, product_key),
    CONSTRAINT fk_fact_customer FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT fk_fact_product  FOREIGN KEY (product_key)  REFERENCES dim_product (product_key),
    CONSTRAINT fk_fact_supplier FOREIGN KEY (supplier_key) REFERENCES dim_supplier (supplier_key),
    CONSTRAINT fk_fact_store    FOREIGN KEY (store_key)    REFERENCES dim_store (store_key),
    CONSTRAINT fk_fact_time     FOREIGN KEY (time_key)     REFERENCES dim_time (time_key),
    CONSTRAINT chk_fact_quantity CHECK (quantity > 0),
    CONSTRAINT chk_fact_revenue CHECK (revenue > 0),
    CONSTRAINT chk_fact_sales_mode CHECK (sales_mode IN ('Online', 'Periodic')),
    CONSTRAINT chk_fact_processing_mode CHECK (processing_mode IN ('Live', 'Periodic', 'Shed'))
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Sales fact. Grain: one product in one shopping cart';


-- =============================================================================
-- Section 4: working tables
--
-- These support the ETL and are not part of the star, so they hold no foreign
-- key to the warehouse. A transaction can sit here long before its dimension
-- rows exist.
-- =============================================================================

-- Master Data R. It stays on disk for the whole run and MESHJOIN reads it one
-- partition of 500 tuples at a time, in primary key order.
CREATE TABLE customer_master_data (
    customer_id                VARCHAR(12) COLLATE utf8mb4_bin NOT NULL,
    gender                     VARCHAR(6)  NOT NULL,
    age_group                  VARCHAR(5)  NOT NULL,
    occupation                 VARCHAR(12) NOT NULL,
    city_category              CHAR(1)     NOT NULL,
    stay_in_current_city_years VARCHAR(2)  NOT NULL,
    marital_status             VARCHAR(7)  NOT NULL,
    PRIMARY KEY (customer_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Customer Master Data R, read in partitions of 500';

CREATE TABLE product_master_data (
    product_id       VARCHAR(20)  COLLATE utf8mb4_bin NOT NULL,
    product_name     VARCHAR(255) NOT NULL,
    product_category VARCHAR(20)  NOT NULL,
    supplier_id      VARCHAR(8)   COLLATE utf8mb4_bin NOT NULL,
    supplier_name    VARCHAR(40)  NOT NULL,
    PRIMARY KEY (product_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Product Master Data R, read in partitions of 500';

-- Staging for the store sales. release_hour is the group number, 0 to 47.
--   released = 1 : the row has left this table, either into the stream buffer or
--                  into shed_table, so the same group is not extracted twice
--   loaded   = 1 : the row has been committed to the fact table
CREATE TABLE periodic_sales_data (
    staging_id     INT UNSIGNED  NOT NULL AUTO_INCREMENT,
    transaction_id VARCHAR(10)   COLLATE utf8mb4_bin NOT NULL,
    customer_id    VARCHAR(12)   COLLATE utf8mb4_bin NOT NULL,
    product_id     VARCHAR(20)   COLLATE utf8mb4_bin NOT NULL,
    quantity       INT           NOT NULL,
    unit_price     DECIMAL(10,3) NOT NULL,
    event_time     DATETIME      NOT NULL,
    sales_mode     VARCHAR(8)    NOT NULL,
    release_hour   SMALLINT      NOT NULL,
    released       TINYINT       NOT NULL DEFAULT 0,
    loaded         TINYINT       NOT NULL DEFAULT 0,
    PRIMARY KEY (staging_id),
    INDEX idx_staging_release (release_hour, released),
    CONSTRAINT chk_staging_mode CHECK (sales_mode = 'Periodic')
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Staging table for the 48 hourly periodic groups';

-- Periodic rows moved to disk during a peak hour. Only periodic rows may be shed
-- (PDF section 12), and the CHECK holds that rule in the database as well. A
-- staging row can be on the shed disk only once, hence the UNIQUE.
CREATE TABLE shed_table (
    shed_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    staging_id     INT UNSIGNED    NOT NULL,
    transaction_id VARCHAR(10)     COLLATE utf8mb4_bin NOT NULL,
    customer_id    VARCHAR(12)     COLLATE utf8mb4_bin NOT NULL,
    product_id     VARCHAR(20)     COLLATE utf8mb4_bin NOT NULL,
    quantity       INT             NOT NULL,
    unit_price     DECIMAL(10,3)   NOT NULL,
    event_time     DATETIME        NOT NULL,
    sales_mode     VARCHAR(8)      NOT NULL,
    shed_time      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (shed_id),
    CONSTRAINT uq_shed_staging UNIQUE (staging_id),
    CONSTRAINT chk_shed_mode CHECK (sales_mode = 'Periodic')
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Periodic transactions shed to disk, with the time they were shed';

-- Bonus (PDF section 16). Live join results wait here, fully enriched, and move
-- to the fact table during a slow period. Periodic results are never cached.
CREATE TABLE live_cache (
    cache_id                   BIGINT UNSIGNED  NOT NULL AUTO_INCREMENT,
    transaction_id             VARCHAR(10)      COLLATE utf8mb4_bin NOT NULL,
    customer_id                VARCHAR(12)      COLLATE utf8mb4_bin NOT NULL,
    gender                     VARCHAR(6)       NOT NULL,
    age_group                  VARCHAR(5)       NOT NULL,
    occupation                 VARCHAR(12)      NOT NULL,
    city_category              CHAR(1)          NOT NULL,
    stay_in_current_city_years VARCHAR(2)       NOT NULL,
    marital_status             VARCHAR(7)       NOT NULL,
    product_id                 VARCHAR(20)      COLLATE utf8mb4_bin NOT NULL,
    product_name               VARCHAR(255)     NOT NULL,
    product_category           VARCHAR(20)      NOT NULL,
    supplier_id                VARCHAR(8)       COLLATE utf8mb4_bin NOT NULL,
    supplier_name              VARCHAR(40)      NOT NULL,
    store_id                   TINYINT UNSIGNED NOT NULL,
    event_time                 DATETIME         NOT NULL,
    quantity                   INT              NOT NULL,
    revenue                    DECIMAL(14,3)    NOT NULL,
    sales_mode                 VARCHAR(8)       NOT NULL,
    processing_mode            VARCHAR(8)       NOT NULL,
    cached_at                  DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (cache_id),
    CONSTRAINT chk_cache_processing_mode CHECK (processing_mode = 'Live')
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Bonus cache of Live join results waiting for a slow period';

-- One row per simulated hour. The shed counts in the report come from here.
CREATE TABLE etl_hourly_log (
    log_id              INT UNSIGNED NOT NULL AUTO_INCREMENT,
    sim_hour            INT          NOT NULL,
    speed_mode          VARCHAR(8)   NOT NULL,
    command_mode        VARCHAR(8)   NOT NULL,
    group_no            INT          NULL,
    group_size          INT          NOT NULL DEFAULT 0,
    admitted            INT          NOT NULL DEFAULT 0,
    shed_out            INT          NOT NULL DEFAULT 0,
    reinjected          INT          NOT NULL DEFAULT 0,
    periodic_loaded     INT          NOT NULL DEFAULT 0,
    shed_loaded         INT          NOT NULL DEFAULT 0,
    live_committed      INT          NOT NULL DEFAULT 0,
    on_shed_disk        INT          NOT NULL DEFAULT 0,
    on_live_cache       INT          NOT NULL DEFAULT 0,
    fact_rows           BIGINT       NOT NULL DEFAULT 0,
    live_latency_p50_ms INT          NULL,
    live_latency_p95_ms INT          NULL,
    iterations          INT          NOT NULL DEFAULT 0,
    avg_iteration_ms    DECIMAL(8,2) NOT NULL DEFAULT 0,
    logged_at           DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (log_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'One summary row per simulated hour';

-- Transactions that cannot become a fact row, kept with the reason so that row
-- accounting still adds up. Columns allow NULL because a row lands here precisely
-- when some value is missing or bad.
CREATE TABLE etl_reject (
    reject_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    transaction_id  VARCHAR(10)     COLLATE utf8mb4_bin NULL,
    customer_id     VARCHAR(12)     COLLATE utf8mb4_bin NULL,
    product_id      VARCHAR(20)     COLLATE utf8mb4_bin NULL,
    quantity        INT             NULL,
    unit_price      DECIMAL(10,3)   NULL,
    event_time      DATETIME        NULL,
    sales_mode      VARCHAR(8)      NULL,
    processing_mode VARCHAR(8)      NULL,
    reason          VARCHAR(40)     NOT NULL,
    rejected_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (reject_id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Rows that could not be loaded, with the reason';

-- One row per writer: prepare, meshjoin, periodic. This table makes restarts
-- exact.
--   txn_seq               : raised inside every write transaction of that writer.
--                           If the connection is lost at COMMIT, reading it back
--                           tells whether that commit really happened.
--   online_rows_committed : resume offset into the online file
--   next_group, sim_hour  : where the hour clock restarts
--   prepared              : set last by data preparation, so a half finished
--                           preparation can never be resumed
CREATE TABLE etl_progress (
    writer                VARCHAR(16) COLLATE utf8mb4_bin NOT NULL,
    txn_seq               BIGINT      NOT NULL DEFAULT 0,
    online_rows_committed BIGINT      NOT NULL DEFAULT 0,
    next_group            INT         NOT NULL DEFAULT 0,
    sim_hour              INT         NOT NULL DEFAULT 0,
    prepared              TINYINT     NOT NULL DEFAULT 0,
    updated_at            DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (writer)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Restart position and commit sequence for each ETL writer';

-- A single row that the MESHJOIN thread refreshes about once a second, so the
-- pipeline can be watched from outside the program.
CREATE TABLE etl_live_status (
    id                  TINYINT     NOT NULL,
    command_mode        VARCHAR(8)  NULL,
    speed_mode          VARCHAR(8)  NULL,
    current_tps         INT         NOT NULL DEFAULT 0,
    sim_hour            INT         NOT NULL DEFAULT 0,
    stream_buffer       INT         NOT NULL DEFAULT 0,
    stream_online       INT         NOT NULL DEFAULT 0,
    stream_periodic     INT         NOT NULL DEFAULT 0,
    stream_shed         INT         NOT NULL DEFAULT 0,
    hash_table          INT         NOT NULL DEFAULT 0,
    queue_chunks        INT         NOT NULL DEFAULT 0,
    customer_partition  INT         NOT NULL DEFAULT 0,
    customer_partitions INT         NOT NULL DEFAULT 0,
    product_partition   INT         NOT NULL DEFAULT 0,
    product_partitions  INT         NOT NULL DEFAULT 0,
    on_shed_disk        INT         NOT NULL DEFAULT 0,
    on_live_cache       INT         NOT NULL DEFAULT 0,
    held_periodic       INT         NOT NULL DEFAULT 0,
    fact_rows           BIGINT      NOT NULL DEFAULT 0,
    live_load           VARCHAR(6)  NULL,
    state               VARCHAR(16) NULL,
    updated_at          DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci
  COMMENT = 'Latest pipeline status, one row';

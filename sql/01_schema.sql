-- =============================================================================
-- NZ FMCG Demand Planning & Inventory Optimisation
-- Step 1: Schema Design
-- =============================================================================
-- Author   : Shinyeong Kim
-- GitHub   : https://github.com/shindatax
-- Context  : Store–DC–Supplier 3-tier supply chain
--            Dimension + Fact tables (Data Warehouse pattern)
-- =============================================================================

CREATE DATABASE IF NOT EXISTS nz_fmcg;
USE nz_fmcg;

-- =============================================================================
-- DIMENSION TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- dim_product: Product master data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_product (
    product_id        VARCHAR(20)    NOT NULL,
    product_name      VARCHAR(100)   NOT NULL,
    category          VARCHAR(50)    NOT NULL,   -- e.g. Dairy, Bakery, Frozen
    subcategory       VARCHAR(50),
    brand             VARCHAR(50),
    unit_of_measure   VARCHAR(20)    NOT NULL,   -- e.g. EA, KG, L
    unit_cost_nzd     DECIMAL(10,2)  NOT NULL,
    unit_price_nzd    DECIMAL(10,2)  NOT NULL,
    shelf_life_days   INT,                       -- NULL = non-perishable
    is_cold_chain     TINYINT(1)     NOT NULL DEFAULT 0,
    is_active         TINYINT(1)     NOT NULL DEFAULT 1,
    created_at        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (product_id),
    INDEX idx_category (category),
    INDEX idx_active (is_active)
) ENGINE=InnoDB COMMENT='Product master — FMCG SKU catalogue';


-- -----------------------------------------------------------------------------
-- dim_store: Retail store master data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_store (
    store_id          VARCHAR(20)    NOT NULL,
    store_name        VARCHAR(100)   NOT NULL,
    store_format      VARCHAR(30)    NOT NULL,   -- e.g. Supermarket, Convenience, Online
    region            VARCHAR(50)    NOT NULL,   -- e.g. Auckland, Wellington, Canterbury
    city              VARCHAR(50)    NOT NULL,
    dc_id             VARCHAR(20)    NOT NULL,   -- Serving distribution centre
    avg_weekly_footfall INT,                     -- Avg customer visits/week
    is_active         TINYINT(1)     NOT NULL DEFAULT 1,
    created_at        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (store_id),
    INDEX idx_region (region),
    INDEX idx_dc (dc_id)
) ENGINE=InnoDB COMMENT='Retail store master — NZ store network';


-- -----------------------------------------------------------------------------
-- dim_dc: Distribution Centre master data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_dc (
    dc_id             VARCHAR(20)    NOT NULL,
    dc_name           VARCHAR(100)   NOT NULL,
    location          VARCHAR(50)    NOT NULL,   -- e.g. Auckland, Christchurch
    region            VARCHAR(50)    NOT NULL,
    capacity_pallets  INT            NOT NULL,
    has_cold_storage  TINYINT(1)     NOT NULL DEFAULT 0,
    supplier_id       VARCHAR(20),               -- Primary supplier
    is_active         TINYINT(1)     NOT NULL DEFAULT 1,
    created_at        DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dc_id),
    INDEX idx_region (region)
) ENGINE=InnoDB COMMENT='Distribution centre master — NZ DC network';


-- -----------------------------------------------------------------------------
-- dim_supplier: Supplier master data
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_supplier (
    supplier_id           VARCHAR(20)    NOT NULL,
    supplier_name         VARCHAR(100)   NOT NULL,
    country               VARCHAR(50)    NOT NULL,   -- e.g. New Zealand, Australia
    lead_time_days        INT            NOT NULL,   -- Standard replenishment lead time
    lead_time_stddev_days DECIMAL(5,2)   NOT NULL DEFAULT 0, -- Lead time variability
    reliability_score     DECIMAL(4,3),              -- 0.000 – 1.000
    min_order_qty         INT            NOT NULL DEFAULT 1,
    is_active             TINYINT(1)     NOT NULL DEFAULT 1,
    created_at            DATETIME       NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (supplier_id),
    INDEX idx_country (country),
    INDEX idx_reliability (reliability_score)
) ENGINE=InnoDB COMMENT='Supplier master — lead time & reliability';


-- -----------------------------------------------------------------------------
-- dim_calendar: Date dimension for time-series analysis
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dim_calendar (
    date_id           DATE           NOT NULL,
    year              SMALLINT       NOT NULL,
    quarter           TINYINT        NOT NULL,   -- 1–4
    month             TINYINT        NOT NULL,   -- 1–12
    month_name        VARCHAR(10)    NOT NULL,
    week_of_year      TINYINT        NOT NULL,   -- 1–53
    day_of_week       TINYINT        NOT NULL,   -- 1=Mon … 7=Sun
    day_name          VARCHAR(10)    NOT NULL,
    is_weekend        TINYINT(1)     NOT NULL DEFAULT 0,
    is_public_holiday TINYINT(1)     NOT NULL DEFAULT 0, -- NZ public holidays
    holiday_name      VARCHAR(50),
    nz_season         VARCHAR(10)    NOT NULL,   -- Summer/Autumn/Winter/Spring
    PRIMARY KEY (date_id),
    INDEX idx_year_month (year, month),
    INDEX idx_season (nz_season)
) ENGINE=InnoDB COMMENT='Date dimension — NZ calendar with public holidays';


-- =============================================================================
-- FACT TABLES
-- =============================================================================

-- -----------------------------------------------------------------------------
-- fact_sales_daily: Daily sales at store level (lowest grain)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_sales_daily (
    sale_id           BIGINT         NOT NULL AUTO_INCREMENT,
    date_id           DATE           NOT NULL,
    store_id          VARCHAR(20)    NOT NULL,
    product_id        VARCHAR(20)    NOT NULL,
    units_sold        INT            NOT NULL DEFAULT 0,
    revenue_nzd       DECIMAL(12,2)  NOT NULL DEFAULT 0.00,
    units_returned    INT            NOT NULL DEFAULT 0,
    is_promotion      TINYINT(1)     NOT NULL DEFAULT 0,
    promo_discount_pct DECIMAL(5,2)  DEFAULT NULL,
    PRIMARY KEY (sale_id),
    UNIQUE KEY uq_daily_sale (date_id, store_id, product_id),
    INDEX idx_date (date_id),
    INDEX idx_store (store_id),
    INDEX idx_product (product_id),
    INDEX idx_store_product (store_id, product_id),
    CONSTRAINT fk_sales_date    FOREIGN KEY (date_id)    REFERENCES dim_calendar(date_id),
    CONSTRAINT fk_sales_store   FOREIGN KEY (store_id)   REFERENCES dim_store(store_id),
    CONSTRAINT fk_sales_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
) ENGINE=InnoDB COMMENT='Daily sales grain — store × product × date';


-- -----------------------------------------------------------------------------
-- fact_inventory_store: Daily end-of-day inventory at store level
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_inventory_store (
    inventory_id      BIGINT         NOT NULL AUTO_INCREMENT,
    date_id           DATE           NOT NULL,
    store_id          VARCHAR(20)    NOT NULL,
    product_id        VARCHAR(20)    NOT NULL,
    qty_on_hand       INT            NOT NULL DEFAULT 0,
    qty_on_order      INT            NOT NULL DEFAULT 0,
    reorder_point     INT            NOT NULL DEFAULT 0,   -- Calculated ROP
    safety_stock      INT            NOT NULL DEFAULT 0,   -- Calculated SS
    is_stockout       TINYINT(1)     NOT NULL DEFAULT 0,   -- 1 = zero stock
    days_of_supply    DECIMAL(6,2),                        -- qty_on_hand / avg_daily_demand
    PRIMARY KEY (inventory_id),
    UNIQUE KEY uq_store_inv (date_id, store_id, product_id),
    INDEX idx_date (date_id),
    INDEX idx_store_product (store_id, product_id),
    INDEX idx_stockout (is_stockout),
    CONSTRAINT fk_sinv_date    FOREIGN KEY (date_id)    REFERENCES dim_calendar(date_id),
    CONSTRAINT fk_sinv_store   FOREIGN KEY (store_id)   REFERENCES dim_store(store_id),
    CONSTRAINT fk_sinv_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
) ENGINE=InnoDB COMMENT='Daily store-level inventory — EOD snapshot';


-- -----------------------------------------------------------------------------
-- fact_inventory_dc: Daily end-of-day inventory at DC level
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_inventory_dc (
    inventory_id      BIGINT         NOT NULL AUTO_INCREMENT,
    date_id           DATE           NOT NULL,
    dc_id             VARCHAR(20)    NOT NULL,
    product_id        VARCHAR(20)    NOT NULL,
    qty_on_hand       INT            NOT NULL DEFAULT 0,
    qty_on_order      INT            NOT NULL DEFAULT 0,
    qty_reserved      INT            NOT NULL DEFAULT 0,   -- Allocated to stores
    qty_available     INT            NOT NULL DEFAULT 0,   -- on_hand - reserved
    reorder_point     INT            NOT NULL DEFAULT 0,
    safety_stock      INT            NOT NULL DEFAULT 0,
    is_stockout       TINYINT(1)     NOT NULL DEFAULT 0,
    PRIMARY KEY (inventory_id),
    UNIQUE KEY uq_dc_inv (date_id, dc_id, product_id),
    INDEX idx_date (date_id),
    INDEX idx_dc_product (dc_id, product_id),
    INDEX idx_stockout (is_stockout),
    CONSTRAINT fk_dinv_date    FOREIGN KEY (date_id)    REFERENCES dim_calendar(date_id),
    CONSTRAINT fk_dinv_dc      FOREIGN KEY (dc_id)      REFERENCES dim_dc(dc_id),
    CONSTRAINT fk_dinv_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
) ENGINE=InnoDB COMMENT='Daily DC-level inventory — EOD snapshot';


-- -----------------------------------------------------------------------------
-- fact_purchase_orders: Purchase orders from DC to Supplier
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_purchase_orders (
    po_id             VARCHAR(30)    NOT NULL,
    dc_id             VARCHAR(20)    NOT NULL,
    supplier_id       VARCHAR(20)    NOT NULL,
    product_id        VARCHAR(20)    NOT NULL,
    order_date        DATE           NOT NULL,
    expected_date     DATE           NOT NULL,
    actual_date       DATE,                                -- NULL = not yet received
    qty_ordered       INT            NOT NULL,
    qty_received      INT,                                 -- NULL = not yet received
    unit_cost_nzd     DECIMAL(10,2)  NOT NULL,
    total_cost_nzd    DECIMAL(14,2)  NOT NULL,
    po_status         VARCHAR(20)    NOT NULL DEFAULT 'OPEN',  -- OPEN/RECEIVED/PARTIAL/CANCELLED
    is_late           TINYINT(1)     NOT NULL DEFAULT 0,
    delay_days        INT            NOT NULL DEFAULT 0,
    PRIMARY KEY (po_id),
    INDEX idx_order_date (order_date),
    INDEX idx_dc_product (dc_id, product_id),
    INDEX idx_supplier (supplier_id),
    INDEX idx_status (po_status),
    CONSTRAINT fk_po_dc       FOREIGN KEY (dc_id)       REFERENCES dim_dc(dc_id),
    CONSTRAINT fk_po_supplier FOREIGN KEY (supplier_id) REFERENCES dim_supplier(supplier_id),
    CONSTRAINT fk_po_product  FOREIGN KEY (product_id)  REFERENCES dim_product(product_id)
) ENGINE=InnoDB COMMENT='Purchase orders — DC to Supplier replenishment';


-- -----------------------------------------------------------------------------
-- fact_shipments: Shipments from DC to Store
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS fact_shipments (
    shipment_id       VARCHAR(30)    NOT NULL,
    dc_id             VARCHAR(20)    NOT NULL,
    store_id          VARCHAR(20)    NOT NULL,
    product_id        VARCHAR(20)    NOT NULL,
    dispatch_date     DATE           NOT NULL,
    expected_date     DATE           NOT NULL,
    actual_date       DATE,                                -- NULL = in transit
    qty_dispatched    INT            NOT NULL,
    qty_received      INT,
    shipment_status   VARCHAR(20)    NOT NULL DEFAULT 'IN_TRANSIT',  -- IN_TRANSIT/DELIVERED/PARTIAL
    is_late           TINYINT(1)     NOT NULL DEFAULT 0,
    delay_days        INT            NOT NULL DEFAULT 0,
    transit_days      INT,                                -- Actual transit time
    PRIMARY KEY (shipment_id),
    INDEX idx_dispatch_date (dispatch_date),
    INDEX idx_dc_store (dc_id, store_id),
    INDEX idx_product (product_id),
    INDEX idx_status (shipment_status),
    CONSTRAINT fk_ship_dc      FOREIGN KEY (dc_id)      REFERENCES dim_dc(dc_id),
    CONSTRAINT fk_ship_store   FOREIGN KEY (store_id)   REFERENCES dim_store(store_id),
    CONSTRAINT fk_ship_product FOREIGN KEY (product_id) REFERENCES dim_product(product_id)
) ENGINE=InnoDB COMMENT='DC-to-Store shipments — replenishment fulfilment';


-- =============================================================================
-- SUMMARY
-- =============================================================================
-- Dimension tables : dim_product, dim_store, dim_dc, dim_supplier, dim_calendar
-- Fact tables      : fact_sales_daily, fact_inventory_store, fact_inventory_dc,
--                    fact_purchase_orders, fact_shipments
-- Total tables     : 10
-- Pattern          : Star schema (DW-style)
-- Engine           : InnoDB (FK support + ACID)
-- =============================================================================

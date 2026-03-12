/*
=============================================================
                 Create Silver Layer Tables
=============================================================

[!] Script Purpose:
    This script creates the core tables of the 'Silver' layer
    in the Data Warehouse architecture.

    The Silver layer is responsible for storing cleaned,
    standardized, and transformed data coming from the
    raw ingestion layer ('Bronze').

    These tables act as an intermediate stage where data
    quality rules, normalization, and structural adjustments
    are applied before being consumed by the analytical
    layer ('Gold').

    The script first checks if each table already exists
    and drops it before recreating it to ensure a clean
    and consistent schema definition.

[!] Tables Created:

    1. silver.crm_cust_info => Stores cleaned customer information including personal details, gender, marital status, and creation date.
    2. silver.crm_prd_info => Stores product information including identifiers, categories, product lines, cost, and validity dates.
    3. silver.crm_sales_details => Stores transactional sales data including order details, quantities, prices, and sales amounts.
    4. silver.erp_loc_a101 => Stores customer location information such as country identifiers.
    5. silver.erp_cust_az12 => Stores additional customer demographic data including birthdate and gender information.
    6. silver.erp_px_cat_g1v2 => Stores product category and subcategory metadata used to enrich product information.

[!] Data Warehouse Architecture:

        Bronze Layer
            Raw source data ingestion

        Silver Layer
            Data cleansing and transformation

        Gold Layer
            Business-ready analytical model
            (dimensions and fact tables)

[X] WARNING:
    Running this script will drop all existing tables
    in the 'silver' schema listed above. All data
    contained in those tables will be permanently
    deleted.

    Ensure proper backups or data reload procedures
    exist before executing this script.
*/


IF OBJECT_ID('silver.crm_cust_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_cust_info;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id             INT,
    cst_key            NVARCHAR(50),
    cst_firstname      NVARCHAR(50),
    cst_lastname       NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr           NVARCHAR(50),
    cst_create_date    DATE,
    dwh_create_date    DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_prd_info', 'U') IS NOT NULL
    DROP TABLE silver.crm_prd_info;
GO

CREATE TABLE silver.crm_prd_info (
    prd_id          INT,
    cat_id          NVARCHAR(50),
    prd_key         NVARCHAR(50),
    prd_nm          NVARCHAR(50),
    prd_cost        INT,
    prd_line        NVARCHAR(50),
    prd_start_dt    DATE,
    prd_end_dt      DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details;
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num     NVARCHAR(50),
    sls_prd_key     NVARCHAR(50),
    sls_cust_id     INT,
    sls_order_dt    DATE,
    sls_ship_dt     DATE,
    sls_due_dt      DATE,
    sls_sales       INT,
    sls_quantity    INT,
    sls_price       INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_loc_a101', 'U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101;
GO

CREATE TABLE silver.erp_loc_a101 (
    cid             NVARCHAR(50),
    cntry           NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12;
GO

CREATE TABLE silver.erp_cust_az12 (
    cid             NVARCHAR(50),
    bdate           DATE,
    gen             NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

IF OBJECT_ID('silver.erp_px_cat_g1v2', 'U') IS NOT NULL
    DROP TABLE silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    id              NVARCHAR(50),
    cat             NVARCHAR(50),
    subcat          NVARCHAR(50),
    maintenance     NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO

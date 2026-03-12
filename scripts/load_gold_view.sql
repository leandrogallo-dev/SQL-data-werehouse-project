/*
=============================================================
        Create Gold Layer Views (Dimensional Model)
=============================================================

[!] Script Purpose:
    This script creates the analytical views that form the
    'Gold' layer of the Data Warehouse. The Gold layer provides
    a business-ready dimensional model optimized for reporting,
    analytics, and BI tools.

    The views are built from the cleaned and transformed data
    stored in the 'silver' schema.

    The script first checks if each view already exists and
    drops it before recreating it to ensure the latest
    definition is applied.

[!] Views Created:

    1. gold.dim_customers
       Dimension table containing enriched customer information
       including demographics, location, and personal attributes.

    2. gold.dim_products
       Dimension table containing product details including
       category, subcategory, product line, and cost information.
       Only current (non-historical) product records are included.

    3. gold.fact_sales
       Fact table containing transactional sales data linked to
       customer and product dimensions.

[!] Data Warehouse Architecture:

        Bronze Layer
            Raw source data ingestion

        Silver Layer
            Data cleansing and transformation

        Gold Layer
            Business-ready dimensional model
            (Star Schema for analytics)

[X] WARNING:
    This script will drop existing Gold layer views if they
    already exist. Any dependent objects (reports, procedures,
    etc.) may be affected if the view structure changes.

=============================================================
*/

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER (ORDER BY cust_table.cst_id) AS customer_key,
	cust_table.cst_id			 AS customer_id,
	cust_table.cst_key			 AS customer_number,
	cust_table.cst_firstname	 AS first_name,
	cust_table.cst_lastname		 AS last_name,
	cust_loc.cntry				 AS country,
	CASE 
		WHEN cust_table.cst_gndr != 'n/a' THEN cust_table.cst_gndr
		ELSE COALESCE(cust_data.gen, 'n/a')
	END AS gender,
	cust_table.cst_marital_status AS marital_status,
	cust_data.bdate				  AS birthdate,
	cust_table.cst_create_date	  AS create_date	
FROM silver.crm_cust_info		  AS cust_table
LEFT JOIN silver.erp_cust_az12 AS cust_data
	ON cust_table.cst_key = cust_data.cid
LEFT JOIN silver.erp_loc_a101 AS cust_loc
	ON cust_table.cst_key = cust_loc.cid;
GO




IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO




IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    ct.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers AS ct
    ON sd.sls_cust_id = ct.customer_id

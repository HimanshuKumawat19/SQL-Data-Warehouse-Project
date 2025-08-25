/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers','V') IS NOT NULL
    DROP VIEW gold.dim_customers
GO
CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_Number() over (ORDER BY cst_id ) AS customer_key, -- surrogate key
    CI.cst_id                            AS customer_id,
    CI.cst_key                           AS customer_number,
    CI.cst_firstname                     AS first_name,
    CI.cst_lastname                      AS last_name,    
    CI.cst_marital_status                AS marital_status,
    CASE 
        WHEN cst_gndr != 'n/a' THEN cst_gndr -- CRM is primary source for gender
        ELSE GEN                             -- fallback for ERP
    END Gender,
    LA.CNTRY                             AS country,
    CA.BDATE                             AS birth_date,
    CI.cst_create_date                   AS create_date,
    CI.dwh_create_date     
    
  FROM silver.crm_cust_info CI
  LEFT JOIN silver.erp_cust_az12 CA
  ON CI.cst_key = CA.CID
  LEFT JOIN silver.erp_loc_a101 LA
  ON CI.cst_key = LA.CID

GO

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID('gold.dim_products','V') IS NOT NULL
	DROP VIEW gold.dim_products;
GO
CREATE VIEW gold.dim_products AS
SELECT 
	ROW_NUMBER() over (ORDER BY prd_id) AS product_key, --surrogate key
	cpi.prd_id			AS product_id,
	cpi.prd_key			AS product_number,
	cpi.prd_nm			AS product_name,
	cpi.prd_cost		AS product_cost,
	pcg.CAT				AS category,	
    pcg.SUBCAT			AS subcategory,
    pcg.MAINTENANCE		AS maintenance,
	cpi.prd_line		AS product_line,
	cpi.prd_start_dt	AS start_date,
	cpi.dwh_create_date
  FROM silver.crm_prd_info cpi
  LEFT JOIN silver.erp_px_cat_g1v2 pcg
  ON cpi.cat_id = pcg.ID
  WHERE cpi.prd_end_dt IS NULL; -- Filter out all historical data

GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================

IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT 
    sd.sls_ord_num     AS order_number,
    dp.product_key,
    dc.customer_key,    
    sd.sls_order_dt    AS order_date,
    sd.sls_ship_dt     AS shipping_date,
    sd.sls_due_dt      AS due_date,
    sd.sls_sales       AS sales,
    sd.sls_quantity    AS quantity,
    sd.sls_price       AS price,
    sd.dwh_create_date
  FROM silver.crm_sales_details sd
  LEFT JOIN gold.dim_products dp
  ON dp.product_number = sd.sls_prd_key
  LEFT JOIN gold.dim_customers dc
  ON dc.customer_id = sd.sls_cust_id

/*
=======================================================================================================================================
Loading Procedure: Load Silver Layer (Bronze -> Silver)
Script Purpose:
This performs the ETL (Extract, Transform, Load) process to populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
- Truncates Silver tables.
- Inserts transformed and cleansed data from Bronze into Silver tables.
=======================================================================================================================================
*/

-- Loading crm_cust_info
TRUNCATE TABLE silver.crm_cust_info;
INSERT INTO silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr,cst_create_date)
WITH flag_ref AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date) AS flag_last
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) AS sub
    WHERE flag_last = 1 -- selecting the most recent record per customer creation date
)
SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,
    TRIM(cst_lastname) AS cst_lastname,
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'Unknown'
    END AS cst_marital_status, --Normalise or standardise marital status values to a readable format
    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        ELSE 'Unknown'
    END AS cst_gndr, --Normalise or standardise gender values to a readable format
    cst_create_date
FROM flag_ref;

-- Loading crm_sales_details
TRUNCATE TABLE silver.crm_sales_details
INSERT INTO silver.crm_sales_details (sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
SELECT
sls_ord_num,
sls_prd_key,
sls_cust_id,
CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt) != 8 THEN NULL
    ELSE TO_DATE(TO_VARCHAR(sls_order_dt), 'YYYYMMDD')
    END AS sls_order_dt,
CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt) != 8 THEN NULL
    ELSE TO_DATE(TO_VARCHAR(sls_ship_dt), 'YYYYMMDD')
    END AS sls_ship_dt,
CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt) != 8 THEN NULL
    ELSE TO_DATE(TO_VARCHAR(sls_due_dt), 'YYYYMMDD')
    END AS sls_due_dt,
CASE WHEN sls_sales IS NULL OR sls_sales <= 0
    OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price)
    ELSE sls_sales
END AS sls_sales, -- recalculating sales if original data is wrong
sls_quantity,
CASE WHEN sls_price IS NULL OR sls_price <= 0
    THEN ROUND(sls_sales/ NULLIF(sls_quantity,0))
    ELSE ROUND(sls_price)
END AS sls_price
FROM bronze.crm_sales_details

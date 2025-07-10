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
TRUNCATE TABLE silver.crm_sales_details;
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
FROM bronze.crm_sales_details;


-- Loading crm_prd_info
TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt )
SELECT 
prd_id,
REPLACE(SUBSTR(prd_key,1,5), '-','_') AS cat_id, -- derive new category ID column by extracting
SUBSTR(prd_key,7, LENGTH(prd_key)) AS prd_key, -- derive new product Key column by extracting
prd_nm,
IFNULL (prd_cost,0) as prd_cost,
CASE UPPER(TRIM(prd_line)) 
    WHEN 'M' THEN 'Mountain'
    WHEN 'R' THEN 'Road'
    WHEN 'S' THEN 'Other Sales'
    WHEN 'T' THEN 'Touring'
    ELSE 'Unknown'
    END AS prd_line,
prd_start_dt,
LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS prd_end_dt -- calculate end date as one day before the next start date
FROM bronze.crm_prd_info;

-- Loading erp_cust_az12
TRUNCATE TABLE silver.erp_cust_az12;
INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
SELECT 
CASE WHEN cid LIKE 'NAS%'
    THEN SUBSTR(cid,4,LENGTH(cid))
    ELSE cid
END AS cid,
CASE WHEN bdate > CURRENT_DATE THEN NULL
    ELSE bdate
END AS bdate,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
     ELSE 'Unknown'
END AS gen
FROM bronze.erp_cust_az12;

-- Loading erp_loc_a101
TRUNCATE TABLE silver.erp_loc_a101;
INSERT INTO silver.erp_loc_a101 (cid,cntry)
SELECT
REPLACE(cid,'-','') cid,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
     WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
     WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'Unknown'
ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101;

-- Loading erp_px_cat_g1v2
TRUNCATE TABLE silver.erp_px_cat_g1v2;
INSERT INTO silver.erp_px_cat_g1v2 (id,cat,subcat,maintenance)
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2;

/*
===========================================
DDL Script: Create Bronze Tables
===========================================
Script purpose:
  This script creates tables in the 'bronze' schema, creating or replacing tables if they already exist.
  Run this script to re-define the DDL structure of 'bronze' Tables
=========================================================================================================
*/

CREATE OR REPLACE TABLE bronze.crm_cust_info (
cst_id INT,
cst_key VARCHAR,
cst_firstname VARCHAR,
cst_lastname VARCHAR,
cst_marital_status VARCHAR,
cst_gndr VARCHAR,
cst_create_date DATE
);

--SELECT COUNT(*) FROM bronze.crm_cust_info

CREATE OR REPLACE TABLE bronze.crm_prd_info (
prd_id INT,
prd_key VARCHAR,
prd_nm VARCHAR,
prd_cost INT,
prd_line VARCHAR,
prd_start_dt DATE,
prd_end_dt DATE
);

CREATE OR REPLACE TABLE bronze.crm_sales_details (
sls_ord_num VARCHAR,
sls_prd_key VARCHAR,
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT, 
sls_sales INT,
sls_quantity INT,
sls_price INT
);

CREATE OR REPLACE TABLE bronze.erp_cust_az12 (
cid VARCHAR,
bdate DATE,
gen VARCHAR
);

CREATE OR REPLACE TABLE bronze.erp_loc_a101 (
cid VARCHAR,
cntry VARCHAR
);

CREATE OR REPLACE TABLE bronze.erp_px_cat_g1v2 (
id VARCHAR,
cat VARCHAR,
subcat VARCHAR,
maintenance VARCHAR
);

/*
====================================================================
DDL Script : Create Silver Tables
====================================================================
Script purpose:
  This script creates tables in the 'silver' schema, and also 
  an additional dwh_create_date column to capture the datetime the 
  data were imported into the tables.
====================================================================
*/

CREATE OR REPLACE TABLE silver.crm_cust_info (
cst_id INT,
cst_key VARCHAR,
cst_firstname VARCHAR,
cst_lastname VARCHAR,
cst_marital_status VARCHAR,
cst_gndr VARCHAR,
cst_create_date DATE,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);


CREATE OR REPLACE TABLE silver.crm_prd_info (
prd_id INT,
prd_key VARCHAR,
prd_nm VARCHAR,
prd_cost INT,
prd_line VARCHAR,
prd_start_dt DATE,
prd_end_dt DATE,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.crm_sales_details (
sls_ord_num VARCHAR,
sls_prd_key VARCHAR,
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT, 
sls_sales INT,
sls_quantity INT,
sls_price INT,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.erp_cust_az12 (
cid VARCHAR,
bdate DATE,
gen VARCHAR,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.erp_loc_a101 (
cid VARCHAR,
cntry VARCHAR,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE TABLE silver.erp_px_cat_g1v2 (
id VARCHAR,
cat VARCHAR,
subcat VARCHAR,
maintenance VARCHAR,
dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

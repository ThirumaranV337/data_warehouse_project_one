--==========================================================================================================================================================================================================================================
  --Quality was checking for the silver.crm_cust_info.
--Quality checking
--checking for unwanted spaces 
--excepting the not result 
select cst_lastname
from silver.crm_cust_info
where cst_lastname!=trim(cst_lastname)
--for first_name
select cst_firstname
from silver.crm_cust_info
where cst_firstname!=trim(cst_firstname)
--checking for the duplicate 
select 
cst_id,
count(*)
from silver.crm_cust_info 
group by cst_id
having count(*)>1
--Data standardization
select Distinct
cst_gndr
from silver.crm_cust_info
--checking for crm_marital_status
select Distinct
cst_marital_status
from silver.crm_cust_info
--===================================================================
--checking the data quality in the crm_prd_info
--Checking for null or duplicate in the primary key 
--Exceptation no result 
Select prd_id
from silver.crm_prd_info
group by prd_id
Having count(*)>1 or prd_id is Null
--checking the name of the product 
--Excepted no result 
select prd_nm
from silver.crm_prd_info
where prd_nm !=Trim(prd_nm)
--checking for Null or Negative Number 
--Excepted no result
Select prd_cost
from silver.crm_prd_info
where prd_cost  <0 or prd_cost is null 
--Data standardization
select Distinct prd_line
from silver.crm_prd_info
--check for invalid date orders 
select * 
from silver.crm_prd_info
where prd_end_dt<prd_start_dt

--==============================================================================
--check for data quality in the crm_sales_info
--==============================================================================

--check for Invalid Dates
SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM silver.crm_sales_info
WHERE sls_order_dt <= 0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20500101
OR sls_order_dt < 19000101

--checking for invalid date orders
SELECT
*
FROM silver.crm_sales_info
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt

/*
Check Data Consistency : Between Sales, Quantity, and Price
Sales = Quantity * Price
Values must not be Null, Zero, or negative.
*/
SELECT DISTINCT
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_info
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
/*
checking the quality of the erp_CUST_AZ12 TABLE 
*/
SELECT DISTINCT 
bdate
FROM silver.erp_cust_az12
WHERE bdate <'1924-01-01' or bdate >GETDATE()
--Data Standardization & consistency 
SELECT DISTINCT gen 
FROM silver.erp_cust_az12
/*
checking the quality of the erp_loc_a101 TABLE 
*/
select distinct cntry
from silver.erp_loc_a101
order by cntry
select * From silver.erp_loc_a101
/*
checking the quality of the erp_PX_CAT_G1V2 TABLE 
*/
SELECT * FROM erp_PX_CAT_G1V2
where cat !=trim(cat) or subcat !=Trim(subcat) or maintenance != Trim(maintenance)
--data standardization 
select distinct
subcat,
cat,
maintenance
from silver.erp_PX_CAT_G1V2

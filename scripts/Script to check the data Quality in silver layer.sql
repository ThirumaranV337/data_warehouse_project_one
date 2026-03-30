==========================================================================================================================================================================================================================================
  Quality was checking for the bronze.crm_cust_info.
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
from bronze.crm_prd_info
group by prd_id
Having count(*)>1 or prd_id is Null
--checking the name of the product 
--Excepted no result 
select prd_nm
from bronze.crm_prd_info
where prd_nm !=Trim(prd_nm)
--checking for Null or Negative Number 
--Excepted no result
Select prd_cost
from bronze.crm_prd_info
where prd_cost  <0 or prd_cost is null 
--Data standardization
select Distinct prd_line
from bronze.crm_prd_info
--check for invalid date orders 
select * 
from bronze.crm_prd_info
where prd_end_dt<prd_start_dt

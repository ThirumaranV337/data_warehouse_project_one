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

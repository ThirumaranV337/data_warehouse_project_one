INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_marital_status,
cst_gndr,
cst_create_date
)
select
 cst_id,
 cst_key,
 trim(cst_firstname) as cst_firstname,
 trim(cst_lastname) as cst_lastname,
 
 case when upper(trim(cst_marital_status))='M' then 'Married'
     when upper(trim(cst_marital_status))='S' then 'Single'
     else 'N/A'
 end cst_marital_status,
 
 case when upper(trim(cst_gndr))='F' then 'Male'
     when upper(trim(cst_gndr))='M' then 'Female'
     else 'N/A'
 end cst_gndr,
 cst_create_date
from
(select *,
ROW_NUMBER() OVER(PARTITION BY cst_id  ORDER BY cst_create_date DESC) as flag_lst
from bronze.crm_cust_info)t
where flag_lst=1
--========================================================================================
select 
prd_id,
prd_key,
replace(substring(prd_key,1,5),'-','_') as cat_id,
substring(prd_key,7,len(prd_key)) as prd_key,
prd_nm,
isnull(prd_cost,0)as prd_cost,

case when upper(trim(prd_line))='M' then 'Mountain'
     when upper(trim(prd_line))='R' then 'Road'
     when upper(trim(prd_line))='S' then 'Other Sales'
     when upper(trim(prd_line))='T' then 'Touring'
     ElSe 'N/A'
end as prd_line,
prd_start_dt,
prd_end_dt
from bronze.crm_prd_info

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
insert into silver.crm_prd_info(
prd_id,
cat_id,
prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt)
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key, -- Renamed to avoid duplicate alias
        prd_nm,
        ISNULL(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A' 
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        -- Use DATEADD to subtract 1 day from the Lead value
        CAST(DATEADD(day, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)) AS DATE) AS prd_end_dt
    FROM bronze.crm_prd_info;
===============================================================================================
INSERT INTO silver.crm_sales_info (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    case when sls_order_dt =0 or LEN(sls_order_dt) !=8 Then NULL
         Else cast(cast(sls_order_dt AS VARCHAR) AS DATE)
    END AS sls_order_dt,
    case when sls_ship_dt =0 or LEN(sls_ship_dt) !=8 Then NULL
         Else cast(cast(sls_ship_dt AS VARCHAR) AS DATE)
    END AS sls_ship_dt,
    case when sls_due_dt =0 or LEN(sls_due_dt) !=8 Then NULL
         Else cast(cast(sls_due_dt AS VARCHAR) AS DATE)
    END AS sls_due_dt, 
    case when sls_sales is null or sls_sales<=0 or sls_sales!=sls_quantity * ABS(sls_price)
            Then sls_quantity *ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    case when sls_price is NUll or sls_price <=0
              Then sls_sales/NULLIF(sls_quantity,0)
         Else sls_price
    End as sls_price
FROM bronze.crm_sales_info
===================================================================================================

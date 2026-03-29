USE data_ware_house;
GO
IF object_id('bronze.crm_cust_info','u') IS Not null ---here we are checking the table already exists or not ,if it exists we delete and create a new 
   drop table bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE
);
GO
IF object_id('bronze.crm_prd_info','u') IS Not null
   drop table bronze.crm_prd_info;

CREATE TABLE bronze.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),   
    prd_cost INT,           
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE
);
USE data_ware_house;
GO
IF object_id('bronze.crm_sales_info','u') IS Not null
   drop table bronze.crm_sales_info;

CREATE TABLE bronze.crm_sales_info (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,      
    sls_ship_dt INT,       
    sls_due_dt INT,       
    sls_sales INT,         
    sls_quantity INT,
    sls_price INT          
);
USE data_ware_house;
GO
IF object_id('bronze.erp_CUST_AZ12','u') IS Not null
   drop table bronze.erp_CUST_AZ12;

CREATE TABLE bronze.erp_CUST_AZ12 (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50)
);
USE data_ware_house;
GO
IF object_id('bronze.erp_LOC_A101','u') IS Not null
   drop table bronze.erp_LOC_A101;

CREATE TABLE bronze.erp_LOC_A101 (
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50)
);
USE data_ware_house;
GO
IF object_id('bronze.erp_PX_CAT_G1V2','u') IS Not null
   drop table bronze.erp_PX_CAT_G1V2;

CREATE TABLE bronze.erp_PX_CAT_G1V2 (
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50)
);

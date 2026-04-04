USE data_ware_house;
GO
IF object_id('silver.crm_cust_info','u') IS Not null
   drop table silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info (
    cst_id INT,
    cst_key NVARCHAR(50),
    cst_firstname NVARCHAR(50),
    cst_lastname NVARCHAR(50),
    cst_marital_status NVARCHAR(50),
    cst_gndr NVARCHAR(50),
    cst_create_date DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
GO
IF object_id('silver.crm_prd_info','u') IS Not null
   drop table silver.crm_prd_info;

CREATE TABLE silver.crm_prd_info (
    prd_id INT,
    prd_key NVARCHAR(50),
    prd_nm NVARCHAR(100),   
    prd_cost INT,           
    prd_line NVARCHAR(50),
    prd_start_dt DATE,
    prd_end_dt DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
USE data_ware_house;
GO
IF object_id('silver.crm_sales_info','u') IS Not null
   drop table silver.crm_sales_info;

CREATE TABLE silver.crm_sales_info (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt INT,      
    sls_ship_dt INT,       
    sls_due_dt INT,       
    sls_sales INT,         
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
USE data_ware_house;
GO
IF object_id('silver.erp_CUST_AZ12','u') IS Not null
   drop table silver.erp_CUST_AZ12;

CREATE TABLE silver.erp_CUST_AZ12 (
    CID NVARCHAR(50),
    BDATE DATE,
    GEN NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
USE data_ware_house;
GO
IF object_id('silver.erp_LOC_A101','u') IS Not null
   drop table silver.erp_LOC_A101;

CREATE TABLE silver.erp_LOC_A101 (
    CID NVARCHAR(50),
    CNTRY NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);
USE data_ware_house;
GO
IF object_id('silver.erp_PX_CAT_G1V2','u') IS Not null
   drop table silver.erp_PX_CAT_G1V2;

CREATE TABLE silver.erp_PX_CAT_G1V2 (
    ID NVARCHAR(50),
    CAT NVARCHAR(50),
    SUBCAT NVARCHAR(50),
    MAINTENANCE NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
);

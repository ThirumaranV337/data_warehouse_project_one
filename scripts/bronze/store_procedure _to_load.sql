/*
---------------------------------------------------------
this store procedure will extract and load the data from to the bronze layer 
*/
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
   DECLARE @start_time DATETIME,@end_time DATETIME;
   BEGIN TRY
    PRINT'=======================';
    PRINT'LOADING CRM TABLES';
    PRINT'=======================';
    SET @start_time=GETDATE();
    BULK INSERT Bronze.crm_cust_info
    FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );
    SELECT COUNT(*) FROM Bronze.crm_cust_info;
    BULK INSERT Bronze.crm_prd_info
    FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );
    BULK INSERT Bronze.crm_slaes_details
    FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );
    SET @end_time=GETDATE();
    PRINT'>>LOAD DURATION:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
    PRINT'=========================';
    PRINT'LOADING ERP TABLES';
    PRINT'========================='
    SET @start_time=GETDATE();

    BULK INSERT Bronze.erp_cust_az12
    FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );
    select * from Bronze.erp_cust_az12;
    
    BULK INSERT Bronze.erp_loc_a101
    FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );
    BULK INSERT Bronze.erp_px_cat_g1v2
    FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH(
        FIRSTROW=2,
        FIELDTERMINATOR=',',
        TABLOCK
    );
    SET @end_time=GETDATE();
   PRINT'>>LOAD DURATION:'+CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR)+'seconds';
    END TRY
    BEGIN CATCH
       PRINT'==============================';
       PRINT'THE ERROR WAS OCCURED';
       PRINT'==============================';
    END CATCH

END

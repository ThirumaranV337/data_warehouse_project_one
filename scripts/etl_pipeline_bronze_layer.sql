/*
In this store procedure I have created to Full load ETL pipe line for the bronze layer and it also indicate the time taken to insert the data ,it is very optimized  
*/
  
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
  DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
  BEGIN TRY
        SET @batch_start_time=GETDATE();
        print'========================================================================================';
        print'Loading the Bronze Layer';
        print'========================================================================================';
        print'----------------------------------------------------------------------------------------';
        SET @start_time=GETDATE();
        print' Loading CRM Tables';
        print'========================================================================================';
        print'Truncating the table :bronze.crm_cust_info';
        print'========================================================================================';
        TRUNCATE TABLE bronze.crm_cust_info
        print'========================================================================================';
        print'Loading the table :bronze.crm_cust_info';
        print'========================================================================================';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        )
        SET @end_time=GETDATE();
        print'>> Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        PRINT'-----------------------------'
        print'========================================================================================';
        print'Truncating the table :bronze.crm_prd_info';
        print'========================================================================================';
        TRUNCATE TABLE bronze.crm_prd_info
        print'========================================================================================';
        print'Loading the table :bronze.crm_prd_info';
        print'========================================================================================';
        SET @start_time=GETDATE();
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        )
        print'========================================================================================';
        print'Truncating the table :bronze.crm_sales_info';
        print'========================================================================================';
        SET @end_time=GETDATE();
        print'>> Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        TRUNCATE TABLE bronze.crm_sales_info
        print'========================================================================================';
        print'Loading the table :bronze.crm_sales_info';
        print'========================================================================================';
        SET @start_time=GETDATE();
        BULK INSERT bronze.crm_sales_info
        FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        )
        print'----------------------------------------------------------------------------------------';
        print' Loading ERP Tables';
        print'========================================================================================';
        print'========================================================================================';
        print'Truncating the table :bronze.erp_CUST_AZ12';
        print'========================================================================================';
        SET @end_time=GETDATE();
        print'>> Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
    
        TRUNCATE TABLE bronze.erp_CUST_AZ12
        print'========================================================================================';
        print'Loading the table :bronze.erp_CUST_AZ12';
        print'========================================================================================';
        SET @start_time=GETDATE();
        BULK INSERT bronze.erp_CUST_AZ12
        FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        )
        print'========================================================================================';
        print'Truncating the table :bronze.erp_LOC_A101';
        print'========================================================================================';
        SET @end_time=GETDATE();
        print'>> Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        TRUNCATE TABLE bronze.erp_LOC_A101
        print'========================================================================================';
        print'Loading the table : bronze.erp_LOC_A101';
        print'========================================================================================';
        SET @start_time=GETDATE();
        BULK INSERT bronze.erp_LOC_A101
        FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        )
        SET @end_time=GETDATE();
        print'>> Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        print'========================================================================================';
        print'Truncating the table :bronze.erp_PX_CAT_G1V2';
        print'========================================================================================';
        
        TRUNCATE TABLE bronze.erp_PX_CAT_G1V2
        print'========================================================================================';
        print'Loading the table : bronze.erp_PX_CAT_G1V2';
        print'========================================================================================';
        SET @start_time=GETDATE();
        BULK INSERT bronze.erp_PX_CAT_G1V2
        FROM 'C:\Users\Thirumaran V\Documents\dbc9660c89a3480fa5eb9bae464d6c07\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH(
            FIRSTROW=2,
            FIELDTERMINATOR=',',
            TABLOCK
        )
        SET @end_time=GETDATE();
        print'>> Load Duration :'+CAST(DATEDIFF(second,@start_time,@end_time)AS NVARCHAR)+'seconds';
        SET @batch_end_time=GETDATE();
        print'>>Total bronze Load Duration :'+CAST(DATEDIFF(second,@batch_start_time,@batch_end_time)AS NVARCHAR)+'seconds';
       END TRY
     BEGIN CATCH
     PRINT'===========================================================================================';
     PRINT'ERROR OCCURED DURING  LOADING BRONZE LAYER';
     PRINT'ERROR Message'+ERROR_MESSAGE();
     PRINT'ERROR Message'+CAST(ERROR_NUMBER() AS NVARCHAR);
     PRINT'ERROR Message'+CAST(ERROR_STATE() AS NVARCHAR);
     PRINT'===========================================================================================';
     END CATCH
END



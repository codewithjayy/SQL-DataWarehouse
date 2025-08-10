/*
==================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==================================================

Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
==================================================
*/

--make stored procedure
create or alter procedure bronze.load_bronze as 
begin
--use the try catch wrror headling
--	declare @start_time datetime, @end_time datetime

--aslo check the whole batch time required for that 
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	BEGIN TRY

	 set @batch_start_time=getdate();
		--load whole data
		--use the finding the time to required to uploaded the data an all

		set @start_time = getdate()
		truncate table bronze.crm_cust_info
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\ajay\OneDrive\Desktop\DA\DA Project\Project1 Data Warehouse\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate()
		print'>> Load the Duration:' +  cast(datediff(second,@start_time, @end_time) as nvarchar) +'seconds';
		print '>>----------------------------------------------------------<<'  


		select count(*) from bronze.crm_cust_info

		---------------------for product-----------------------
		set @start_time = getdate()
		truncate table bronze.crm_prd_info
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\ajay\OneDrive\Desktop\DA\DA Project\Project1 Data Warehouse\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		set @end_time = getdate()
		print'>> Load the Duration:' +  cast(datediff(second,@start_time, @end_time) as nvarchar) +'seconds';
		print '>>----------------------------------------------------------<<'  

		select * from bronze.crm_prd_info

		--------------------------------------------for sales------------
		set @start_time = getdate()
		truncate table bronze.crm_sales_details

		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\ajay\OneDrive\Desktop\DA\DA Project\Project1 Data Warehouse\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		set @end_time = getdate()
		print'>> Load the Duration:' +  cast(datediff(second,@start_time, @end_time) as nvarchar) +'seconds';
		print '>>----------------------------------------------------------<<'  

		select * from bronze.crm_sales_details











		------------load the data in the erp------------------
		set @start_time = getdate()
		truncate table bronze.erp_cust_az12

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\ajay\OneDrive\Desktop\DA\DA Project\Project1 Data Warehouse\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate()
		print'>> Load the Duration:' +  cast(datediff(second,@start_time, @end_time) as nvarchar) +'seconds';
		print '>>----------------------------------------------------------<<'  

		select * from bronze.erp_cust_az12

		----------------------------------------------------------------------------------
		set @start_time = getdate()
		truncate table bronze.erp_loc_a101

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\ajay\OneDrive\Desktop\DA\DA Project\Project1 Data Warehouse\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate()
		print'>> Load the Duration:' +  cast(datediff(second,@start_time, @end_time) as nvarchar) +'seconds';
		print '>>----------------------------------------------------------<<'  

		select * from bronze.erp_loc_a101
		-------------------------------------------------------------------------------------------------
		set @start_time = getdate()
        truncate table bronze.erp_px_cat_g1v2

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\ajay\OneDrive\Desktop\DA\DA Project\Project1 Data Warehouse\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		set @end_time = getdate()
		print'>> Load the Duration:' +  cast(datediff(second,@start_time, @end_time) as nvarchar) +'seconds';
		print '>>----------------------------------------------------------<<'  

		select * from bronze.erp_px_cat_g1v2

		--end the batch time here

	set @batch_end_time=getdate();
	print '=========================================='
	print 'loading bronze layer is completed'
	print'>> Load the Duration:' +  cast(datediff(second,@batch_start_time, @batch_end_time) as nvarchar) +'seconds';
	print '>>----------------------------------------------------------<<'  


	END TRY
	BEGIN CATCH
		PRINT '==================================================';
		PRINT 'ERROR OCCURRED DURING LOADING BRONZE LAYER';
		PRINT 'Step: Data Ingestion from source_crm to bronze layer';
		PRINT 'Possible Cause: File not found, invalid file format, schema mismatch, or permission issues.';
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number : ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State  : ' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS NVARCHAR);
		PRINT 'Error Line   : ' + CAST(ERROR_LINE() AS NVARCHAR);
		PRINT 'Recommendation: Check source file path, verify data schema, and ensure proper access rights.';
		PRINT '==================================================';
	END CATCH


END

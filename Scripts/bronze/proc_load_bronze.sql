
/*
Stored Procedure: Load Bronze layer (source -> bronze)
========================================================
Script Purpose: 
This Stored Procedure loads data into the 'bronze' schema from external csv files.
It performs the following actions:
- Truncate the bronze tables before loading.
- Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.
*/


-- 1. Check if the procedure exists and delete (DROP) it if it does
IF OBJECT_ID('bronze.load_bronze', 'P') IS NOT NULL
    DROP PROCEDURE bronze.load_bronze;
GO

-- 2. Now create the procedure cleanly
CREATE PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;  
    BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT('======================================================');
		PRINT('Loading Bronze Layer');
		PRINT('======================================================');

		PRINT('-------------------------------------------------------');
		PRINT('Loading Data for CRM Tables');
		PRINT('-------------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: crm_cust_info');
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT('>> Inserting Data Into Table: crm_cust_info');
		BULK INSERT bronze.crm_cust_info
		FROM 'F:\DataEngineering_Track\Projects\Datasets\CRM\cust_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds');

		
		-- Loading data for prd_info --
		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: crm_prd_info');
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT('>> Inserting Data Into Table: crm_prd_info');
		BULK INSERT bronze.crm_prd_info
		FROM 'F:\DataEngineering_Track\Projects\Datasets\CRM\prd_info.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		
		SET @end_time = GETDATE();
		print('>> Load Duration ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds');
		
		-- Loading data for SALES_DETAILS --
		SET @start_time = GETDATE();

		PRINT('>> Truncating Table: crm_sales_details');
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT('>> Inserting Data Into Table: crm_sales_details');
		BULK INSERT bronze.crm_sales_details
		FROM 'F:\DataEngineering_Track\Projects\Datasets\CRM\sales_details.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds');

		PRINT('-------------------------------------------------------');
		PRINT('Loading Data for ERP Tables');
		PRINT('-------------------------------------------------------');

		-- Loading data for CUST_AZ12 --
		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: erp_cust_az12');
		TRUNCATE TABLE bronze.erp_cust_az12;

		PRINT('>> Inserting Data Into Table: erp_cust_az12');
		BULK INSERT bronze.erp_cust_az12
		FROM 'F:\DataEngineering_Track\Projects\Datasets\ERP\CUST_AZ12.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds');

		-- Loading data for loc_a101 --
        SET @start_time = GETDATE();	
		PRINT('>> Truncating Table: erp_loc_a101');
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT('>> Inserting Data Into Table: erp_loc_a101');
		BULK INSERT bronze.erp_loc_a101
		FROM 'F:\DataEngineering_Track\Projects\Datasets\ERP\LOC_A101.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds');

		-- Loading data for px_cat_g1v2 --
		SET @start_time = GETDATE();
		PRINT('>> Truncating Table: erp_px_cat_g1v2');
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT('>> Inserting Data Into Table: px_cat_g1v2');
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'F:\DataEngineering_Track\Projects\Datasets\ERP\PX_CAT_G1V2.CSV'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		print('>> Load Duration ' + cast(datediff(second,@start_time,@end_time) AS NVARCHAR) + 'Seconds');
	SET @batch_end_time = GETDATE();
	print ' Loading bronze layer is completed';
	print '- Total Load Duration: ' + CAST(datediff(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'Seconds';
	print '==============================================='

	END TRY
	BEGIN CATCH
	PRINT('=================================================')
	PRINT'ERROR OCURRED DURING LOADING BRONZE LAYER'
	PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT('=================================================')
	END CATCH
END

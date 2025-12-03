EXEC [SILVER].[load_silver]

-- 2. Now create the procedure cleanly
CREATE PROCEDURE Silver.load_silver
AS
BEGIN
     DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;  
     BEGIN TRY
	 SET @batch_start_time = GETDATE();
	 print'==============================================';
	 print 'Loading Silver Layer';
     print'==============================================';

	 print'==============================================';
	 print 'Loading CRM Tables';
     print'==============================================';


-- Loading silver.crm_cust_info
SET @start_time = GETDATE();
Print '>> Truncating Table Silver.CRM_Cst_INFO ';
TRUNCATE TABLE [SILVER].[crm_cust_info];
Print '>>Inserting Data into Table Silver.CRM_Cst_INFO'

INSERT INTO [SILVER].[crm_cust_info](cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gndr,
cst_create_date) 
SELECT 
cst_id,
cst_key,
LTRIM(RTRIM(cst_firstname)) as cst_first_name,
LTRIM(RTRIM(cst_lastname)) as cst_last_name,

CASE WHEN UPPER(cst_material_status) = 'S' THEN 'Single'
     WHEN UPPER(cst_material_status) = 'M' THEN 'Married' 
	 ELSE 'N/A'
END cst_material_status,

CASE WHEN UPPER(cst_gndr) = 'F' THEN 'Female'
     WHEN UPPER(cst_gndr) = 'M' THEN 'Male' 
	 ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM 
(
SELECT *,

ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM [BRONZE].[crm_cust_info]
)t where flag_last =1
SET @end_time = GETDATE();
 PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
 PRINT '>> -------------';
  
--Loading Silver.crm_prod_info
SET @start_time = GETDATE();
Print '>> Truncating Table Silver.CRM_PRD_INFO ';
TRUNCATE TABLE [SILVER].[crm_prd_info];
Print '>>Inserting Data into Table Silver.CRM_PRD_INFO';

INSERT INTO [SILVER].[crm_prd_info](
prd_id, 
prd_key, 
cat_id, 
prd_nm,
prd_cost, 
prd_line, 
prd_start_dt, 
prd_end_dt
)
SELECT
prd_id, 
REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id,  -- Extract Category ID
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,        -- Extract Product Key
prd_nm, 
ISNULL(prd_cost,0) AS prd_cost, 
 
CASE WHEN UPPER(prd_line) = 'M' THEN 'Mountain'
     WHEN UPPER(prd_line) = 'R' THEN 'Road'
	 WHEN UPPER(prd_line) = 'S' THEN 'Other Sales'
     WHEN UPPER(prd_line) = 'T' THEN 'Touring'
     Else 'N/A'
END as prd_line,

prd_start_dt,
LEAD([prd_start_dt])over(partition by prd_key order by prd_start_dt) AS prd_end_dt 

FROM [BRONZE].[crm_prd_info];
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

--Loading CRM_Sales_Details
SET @start_time = GETDATE();
Print '>> Truncating Table SILVER.crm_sales_details';
TRUNCATE TABLE [SILVER].[crm_sales_details];
Print '>>Inserting Data into Table SILVER.crm_sales_details';


INSERT INTO [SILVER].[crm_sales_details](
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

CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
     ELSE cast(cast(sls_order_dt as VARCHAR) as DATE)
END AS sls_order_dt,


CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
     ELSE cast(cast(sls_ship_dt as VARCHAR) as DATE)
END AS sls_ship_dt,


CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
     ELSE cast(cast(sls_due_dt as VARCHAR) as DATE)
END AS sls_due_dt,

sls_quantity, 

CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
     THEN  sls_quantity * ABS(sls_price)
     ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <= 0
         THEN sls_sales / NULLIF(sls_quantity,0)
	 ELSE sls_price
END AS sls_price


from [BRONZE].[crm_sales_details];
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

PRINT '------------------------------------------------';
PRINT 'Loading ERP Tables';
PRINT '------------------------------------------------';

-- Loading erp_cust_az12
SET @start_time = GETDATE();
Print '>> Truncating Table SILVER.erp_cust_az12';
TRUNCATE TABLE [SILVER].[erp_cust_az12];
Print '>>Inserting Data into Table SILVER.erp_cust_az12';
-- Customer Information From ERP
INSERT INTO [SILVER].[erp_cust_az12](
cid, 
bdate, 
gen )
SELECT

CASE WHEN cid like 'NAS%' THEN SUBSTRING(cid,4,len(cid))
     ELSE cid
END AS cid,

CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END AS bdate,

CASE WHEN gen IN('MALE','M')      THEN 'Male'
     WHEN gen IN('FEMALE','F')    THEN 'Female'
     ELSE 'N/A'
END AS gen

from [BRONZE].[erp_cust_az12];
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';


-- Loading erp_loc_a101
SET @start_time = GETDATE();
Print '>> Truncating Table SILVER.erp_loc_a101';
TRUNCATE TABLE [SILVER].[erp_loc_a101];
Print '>>Inserting Data into Table SILVER.erp_loc_a101';

--Location Information 
INSERT INTO [SILVER].[erp_loc_a101](
cid,
cntry
)

SELECT
REPLACE(cid,'-','') cid, 


CASE WHEN cntry = 'DE'                  THEN 'Germany'
     WHEN cntry IN ('US' , 'USA')       THEN 'United state' 
	 when cntry = ''  OR cntry IS NULL  THEN 'N/A' 
	 ELSE cntry
END AS cntry

from [BRONZE].[erp_loc_a101];
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------'; 


-- Loading erp_px_cat_g1v2
SET @start_time = GETDATE();
Print '>> Truncating Table SILVER.erp_px_cat_g1v2';
TRUNCATE TABLE [SILVER].[erp_px_cat_g1v2];
Print '>>Inserting Data into Table SILVER.erp_px_cat_g1v2';
-- Product category
INSERT INTO [SILVER].[erp_px_cat_g1v2](
ID,
CAT, 
SUBCAT,
MAINTENANCE
)

SELECT 
ID,
CAT, 
SUBCAT,
MAINTENANCE


from [BRONZE].[erp_px_cat_g1v2];
SET @end_time = GETDATE();
PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
PRINT '>> -------------';

	SET @batch_end_time = GETDATE();
	PRINT '=========================================='
	PRINT 'Loading Silver Layer is Completed';
    PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
    PRINT '=========================================='
END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH


END
GO

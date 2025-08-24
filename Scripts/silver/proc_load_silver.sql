/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
		PRINT '==========================================================================';
		PRINT 'LOADING SILVER DATA';
		PRINT '==========================================================================';

		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------------------------------------';
		-- Setting the start time of batch
		SET @batch_start_time = GETDATE(); 
		-- setting start time
		SET @start_time = GETDATE()
		-- Loading silver.crm_cust_info
		PRINT 'Truncating Table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT 'Inserting Data Into : silver.crm_cust_info';
		
		INSERT INTO silver.crm_cust_info (
					cst_id, 
					cst_key, 
					cst_firstname, 
					cst_lastname, 
					cst_marital_status, 
					cst_gndr,
					cst_create_date
				)
				SELECT
					cst_id,
					cst_key,
					TRIM(cst_firstname) AS cst_firstname,
					TRIM(cst_lastname) AS cst_lastname,
					CASE 
						WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
						WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
						ELSE 'n/a'
					END AS cst_marital_status, -- Normalize marital status values to readable format
					CASE 
						WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
						WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
						ELSE 'n/a'
					END AS cst_gndr, -- Normalize gender values to readable format
					cst_create_date
				FROM (
					SELECT
						*,
						ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
					FROM bronze.crm_cust_info
					WHERE cst_id IS NOT NULL
				) t
				WHERE flag_last = 1; -- Select the most recent record per customer

		-- setting end time
		SET @end_time = GETDATE()
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------'
		-- setting start time
		SET @start_time = GETDATE();
		-- Loading silver.crm_prd_info
		PRINT 'Truncating Table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT 'Inserting Data Into : silver.crm_prd_info';
		

		INSERT INTO silver.crm_prd_info(
					prd_id,          
					cat_id,          
					prd_key,         
					prd_nm,          
					prd_cost,        
					prd_line,        
					prd_start_dt,    
					prd_end_dt      
				)
				SELECT
					prd_id, -- no duplicate
					REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, -- substitute the Category ID
					SUBSTRING(prd_key,7,len(prd_key)) AS prd_key, -- remaining as Product key
					prd_nm,
					ISNULL(prd_cost,0) AS prd_cost, -- replacing NULL with zero
					CASE UPPER(TRIM(prd_line))  -- normalize the product line with readable format
						WHEN 'M' THEN 'Mountains'
						WHEN 'S' THEN 'Other Sales'
						WHEN 'R' THEN 'Road'
						WHEN 'T' THEN 'Touring'
						ELSE 'N/A'
					END AS prd_line,
					CAST(prd_start_dt AS DATE) AS prd_start_dt, -- casting to the DATE datatype
					DATEADD(DAY, -1, 
							LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)
							) AS prd_end_dt -- subtracting 1 day from the next start date
				FROM bronze.crm_prd_info;

		-- setting end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------'
		-- setting start time
		SET @start_time = GETDATE();
		-- loading silver.crm_sales_details
		PRINT 'Truncating Table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT 'Inserting Data Into : silver.crm_sales_details';

		

		INSERT INTO silver.crm_sales_details (
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
								-- Casting from integer to date 
								CASE 
									WHEN sls_order_dt = 0 OR len(sls_order_dt) != 8 THEN NULL
									ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
								END sls_order_dt,
		
								CASE 
									WHEN sls_ship_dt = 0 OR len(sls_ship_dt) != 8 THEN NULL
									ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
								END sls_ship_dt,
		
								CASE 
									WHEN sls_due_dt = 0 OR len(sls_due_dt) != 8 THEN NULL
									ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
								END sls_due_dt,

								-- Sales mathematics correction

								CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
											THEN sls_quantity * ABS(sls_price)
									ELSE sls_sales
								END sls_sales,
								sls_quantity,
								CASE WHEN sls_price IS NULL OR sls_price <= 0
											THEN sls_sales / NULLIF(sls_quantity,0)
									ELSE sls_price
								END sls_price
						FROM bronze.crm_sales_details

		-- setting end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------'

		PRINT '--------------------------------------------------------------------------';
		PRINT 'Loading ERT Tables';
		PRINT '--------------------------------------------------------------------------';

		-- setting start time
		SET @start_time = GETDATE();
		-- loading silver.erp_cust_az12
		PRINT 'Truncating Table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT 'Inserting Data Into : silver.erp_cust_az12';

		INSERT INTO silver.erp_cust_az12 (
					cid,
					bdate,
					gen
				)
					SELECT 
						CASE
							WHEN UPPER(TRIM(CID)) LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(CID))
							ELSE TRIM(CID)
						END CID,
						BDATE,
						CASE 
							WHEN UPPER(TRIM(GEN)) = 'F' THEN 'Female'
							WHEN UPPER(TRIM(GEN)) = 'M' THEN 'Male'
							WHEN GEN IS NULL OR UPPER(TRIM(GEN)) = '' THEN 'N/A'
							ELSE TRIM(GEN)
						END GEN
					FROM bronze.erp_cust_az12;
		-- setting end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------'

		-- setting start time
		SET @start_time = GETDATE();

		-- loading silver.erp_loc_a101
		PRINT 'Truncating Table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT 'Inserting Data Into : silver.erp_loc_a101';

		INSERT INTO silver.erp_loc_a101 (
					cid,
					cntry
				)
					SELECT 
						CID,
						CASE 
							WHEN UPPER(TRIM(CNTRY)) = 'DE' THEN 'GERMANY'
							WHEN UPPER(TRIM(CNTRY)) IN ('USA','US') THEN 'UNITED STATES'
							WHEN UPPER(TRIM(CNTRY)) IS NULL OR UPPER(TRIM(CNTRY)) = ''THEN 'N/A'
							ELSE UPPER(TRIM(CNTRY))
						END CNTRY
					FROM bronze.erp_loc_a101;
		-- setting end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------'

		-- setting start time
		SET @start_time = GETDATE();
		-- loading silver.erp_px_cat_g1v2
		PRINT 'Truncating Table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT 'Inserting Data Into : silver.erp_px_cat_g1v2';

		INSERT INTO silver.erp_px_cat_g1v2 (
					id,
					cat,
					subcat,
					maintenance
				)
					SELECT 
						ID,
						CAT,
						SUBCAT,
						MAINTENANCE
					FROM bronze.erp_px_cat_g1v2;
		-- setting end time
		SET @end_time = GETDATE();
		PRINT '>> Load Duration ' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + ' Seconds';
		PRINT '----------------------------------'

		SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERROR OCCURED WHILE LAODING DATA INTO SILVER TABLES';
		PRINT 'ERROR MESSAGE' + ERROR.MESSAGE();
		PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=======================================================';
	END CATCH 
END

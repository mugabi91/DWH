-- /*
-- 	This script creates the silver layer tables and applies Transformations as stated below for each
-- 	
-- */
USE DWH;

GO

CREATE OR ALTER PROCEDURE Load_silver_Layer AS
	BEGIN
		PRINT '';
		PRINT '==============================================================================';
		PRINT '                           SILVER LAYER STARTING                               ';
		PRINT '==============================================================================';
		PRINT '';

		-- TIME SETUP 
		DECLARE @start_time DATETIME, @end_time DATETIME;

		-- overall start time setup 
		SET @start_time = GETDATE();

		-- crm_sales table begin.
		PRINT '==============================================================================';
		PRINT 'TABLE: silver.crm_sales, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';


		IF OBJECT_ID('silver.crm_sales') IS NOT NULL
			BEGIN
				PRINT 'Dropping silver.crm_Sales table';
				DROP TABLE silver.crm_sales;
				PRINT 'Dropped silver.crm_sales';
			END 
		ELSE
			PRINT 'silver.crm_sales does not exist';

		PRINT 'Creating silver.crm_sales table';
		CREATE TABLE [silver].crm_sales(
				[crm_order_id] NVARCHAR(100),
				[crm_product_key] NVARCHAR(100),
				[crm_customer_id] NVARCHAR(100),
				[crm_order_date] DATE,
				[crm_ship_date] DATE,
				[crm_due_date] DATE,
				[crm_price] FLOAT,
				[crm_quantity] INT,
				[crm_revenue] FLOAT,
				[dwh_created_at] DATETIME
		);
		-- insert silver layer data for table silver.crm_sales
		BEGIN TRY
			WITH clean_crm_sales AS (
				SELECT 
					CAST(TRIM(UPPER([sls_ord_num])) AS NVARCHAR) AS order_id,
					CAST(TRIM(UPPER([sls_prd_key])) AS NVARCHAR) AS product_key,
					CAST(TRIM(UPPER([sls_cust_id])) AS NVARCHAR) AS customer_id,
					TRY_CONVERT(DATE ,[sls_order_dt], 112) AS order_date,
					TRY_CONVERT(DATE ,[sls_ship_dt], 112) AS ship_date,
					TRY_CONVERT(DATE ,[sls_due_dt], 112) AS due_date,
					CAST(
						CASE 
							WHEN [sls_price] IS NULL AND [sls_quantity] IS NOT NULL AND [sls_sales] IS NOT NULL
								THEN ABS([sls_sales])/ NULLIF(ABS([sls_quantity]),0)
							ELSE ABS([sls_price])
						END
						AS FLOAT) AS price,
					CAST(
						CASE 
							WHEN [sls_quantity] IS NULL AND [sls_sales] IS NOT NULL AND [sls_price] IS NOT NULL 
								THEN ABS([sls_sales])/ NULLIF(ABS([sls_price]),0)
							ELSE ABS([sls_quantity])
						END AS INT) AS quantity,
					CAST(
						CASE
							WHEN ABS([sls_quantity]) IS NOT NULL AND ABS([sls_price]) IS NOT NULL AND [sls_quantity] * [sls_price] != [sls_sales] 
								THEN ABS([sls_quantity]) * ABS([sls_price])
							ELSE ABS([sls_sales])
						END AS FLOAT 
						) AS revenue,
					ROW_NUMBER() OVER (
						PARTITION BY CAST(TRIM(UPPER([sls_prd_key])) AS NVARCHAR),
						CAST(TRIM(UPPER([sls_cust_id])) AS NVARCHAR),
						TRY_CONVERT(DATE ,[sls_order_dt], 112)
						ORDER BY CAST(TRIM(UPPER([sls_prd_key])) AS NVARCHAR) ASC
						) AS row_num,
					GETDATE() AS dwh_created_at
				FROM bronze.crm_sales),

				Final AS (
					SELECT 
						[order_id],
						[product_key],
						[customer_id],
						[order_date],
						[ship_date],
						[due_date],
						[price],
						[quantity],
						[revenue],
						[dwh_created_at]
					FROM clean_crm_sales 
				)
			INSERT INTO silver.crm_sales(
				[crm_order_id],
				[crm_product_key],
				[crm_customer_id],
				[crm_order_date],
				[crm_ship_date] ,
				[crm_due_date] ,
				[crm_price] ,
				[crm_quantity],
				[crm_revenue] ,
				[dwh_created_at]
			) 
			SELECT * FROM Final;

			PRINT 'Created, Transformed and loaded successfully TABLE: silver.crm_sales'
		END TRY
		BEGIN CATCH
			PRINT 'Table data insertion ERROR: silver.crm_sales';
			PRINT 'ERROR:'+ ERROR_MESSAGE();
		END CATCH;

		PRINT 'Data import successfully table: silver.crm_sales';
		PRINT '==============================================================================';
		PRINT '';
		PRINT '==============================================================================';

		-- -- crm_sales table end



		-- prd_info table begin.

		PRINT 'TABLE: silver.crm_prd_info, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';


		IF OBJECT_ID('silver.crm_prd_info') IS NOT NULL
			BEGIN
				PRINT 'Dropping silver.crm_prd_info table';
				DROP TABLE silver.crm_prd_info;
				PRINT 'Dropped silver.crm_prd_info table';
			END
			ELSE
				PRINT 'silver.crm_prd_info table doesnot exist';
		PRINT 'Creating silver.crm_prd_info table';
		CREATE TABLE silver.crm_prd_info(
				crm_prd_cat_id NVARCHAR(255),
				crm_prd_key NVARCHAR(255),
				crm_prd_name NVARCHAR(255),
				crm_prd_cost FLOAT,
				crm_prd_line NVARCHAR(255),
				crm_prd_start_date DATE,
				crm_prd_end_date DATE,
				dwh_created_at DATETIME
			);
		-- insert silver layer data for table silver.crm_prd_info
		BEGIN TRY
			WITH clean_crm_prd_info AS(
				SELECT 
					CAST(SUBSTRING(UPPER(TRIM(prd_key)),1,5) AS NVARCHAR) AS prd_cat_id,
					CAST(SUBSTRING(UPPER(TRIM(prd_key)),7,LEN(UPPER(TRIM(prd_key)))) AS NVARCHAR) AS prd_key,
					CAST(TRIM(prd_nm)AS NVARCHAR) AS prd_nm,
					CAST(prd_cost AS FLOAT ) AS prd_cost,
					CASE 
						WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
						WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
						WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
						WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
						ELSE 'n/a'
					END AS prd_line,
					CASE
						WHEN TRY_CONVERT(DATE, prd_start_dt) IS NOT NULL AND TRY_CONVERT(DATE, prd_end_dt) IS NOT NULL AND
							TRY_CONVERT(DATE, prd_end_dt) <= TRY_CONVERT(DATE, prd_start_dt)
							THEN TRY_CONVERT(DATE, prd_end_dt)
						WHEN prd_start_dt IS NULL THEN 'N/A'
						ELSE
							TRY_CONVERT(DATE, prd_start_dt)
					END AS prd_start_dt,
					CASE
						WHEN prd_end_dt  IS NULL THEN NULL
						WHEN prd_start_dt IS NOT NULL AND TRY_CONVERT(DATE, prd_start_dt) >= TRY_CONVERT(DATE, prd_end_dt)
						THEN TRY_CONVERT(DATE, prd_start_dt)
					END AS prd_end_dt,
					ROW_NUMBER() OVER( PARTITION BY TRY_CONVERT(DATE, prd_start_dt) 
					ORDER BY TRY_CONVERT(DATE, prd_start_dt) ASC) AS row_num,
					GETDATE() AS dwh_created_at
				FROM bronze.crm_prd_info 
				),

				Final AS(
					SELECT
						prd_cat_id,
						prd_key,
						prd_nm,
						prd_cost,
						prd_line,
						prd_start_dt,
						prd_end_dt,
						dwh_created_at
					FROM clean_crm_prd_info
				)INSERT INTO silver.crm_prd_info(
					crm_prd_cat_id,
					crm_prd_key,
					crm_prd_name,
					crm_prd_cost,
					crm_prd_line,
					crm_prd_start_date,
					crm_prd_end_date,
					dwh_created_at
				)
				SELECT * FROM Final;
		END TRY
		BEGIN CATCH
			PRINT 'Table data insertion ERROR: silver.crm_prd_info';
			PRINT 'ERROR:'+ ERROR_MESSAGE();
		END CATCH;

		PRINT 'Data import successfully table: silver.crm_prd_info';
		PRINT '==============================================================================';



		PRINT '';
		PRINT '==============================================================================';

		-- prd_info table end.

		-- silver.cust_info table begin.

		/*
				----------------------------------------------------------------------------------------------------------------------------------------
				----------------------------------------------------------------------------------------------------------------------------------------
				-- Silver.crm_cust_info table transformations 
			
				Cte_crm_cust_info groups the customers by there cust_id and counting how many times they appear in the 
				data then filters out any customers that appear more than once. Then its does some column  specific 
				data cleaning:
					cst_id: cast it to numeric
					cst_key: upper case and trim
					cst_firstname:Trim
					cst_marital_status: Upper and trim
					cst_gndr: Upper and trim 
					cst_date:none
					created_at: Added meta data

		*/
	
		PRINT 'TABLE: silver.crm_cust_info, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		IF OBJECT_ID('silver.crm_cust_info') IS NOT NULL
			BEGIN
				PRINT 'Dropping silver.crm_cust_info table';
				DROP TABLE silver.crm_cust_info;
				PRINT 'Dropped silver.crm_cust_info table';
			END
			ELSE
				PRINT 'silver.crm_cust_info table doesnot exist';
		PRINT 'Creating silver.crm_cust_info table';
		CREATE TABLE silver.crm_cust_info(
			[crm_cst_id] INT,
			[crm_cst_key] NVARCHAR(50),
			[crm_cst_firstname] NVARCHAR(50),
			[crm_cst_lastname] NVARCHAR(50),
			[crm_cst_marital_status] NVARCHAR(10),
			[crm_cst_gender] NVARCHAR(6),
			[crm_cst_create_date] DATE,
			[dwh_created_at] DATETIME
		);

		-- Step 1: Create Temp Table 1
		DROP TABLE IF EXISTS #temp_table_1, #temp_table_2;

		SELECT 
			CAST(cst_id AS NUMERIC) AS cst_id,
			TRIM(UPPER(cst_key)) AS cst_key,
			TRIM(cst_firstname) AS cst_firstname,  
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN TRIM(UPPER([cst_marital_status])) = 'S' THEN 'Single'
				WHEN TRIM(UPPER([cst_marital_status])) = 'M' THEN 'Married'
				WHEN TRIM(UPPER([cst_marital_status])) = 'D' THEN 'Divorced'
				ELSE NULL
			END AS cst_marital_status,
			CASE
				WHEN TRIM(UPPER(cst_gndr)) = 'M' THEN 'Male'
				WHEN TRIM(UPPER(cst_gndr)) = 'F' THEN 'Female'
				ELSE NULL
			END AS cst_gndr,
			cst_create_date AS cst_create_date,
			GETDATE() AS Created_at 
		INTO #temp_table_1
		FROM bronze.crm_cust_info
		WHERE cst_id NOT IN ( 
			SELECT t.rep_cst_id 
			FROM (
				SELECT 
					TRIM(cst_id) AS rep_cst_id, 
					COUNT(TRIM(cst_id)) AS n_count 
				FROM bronze.crm_cust_info 
				GROUP BY cst_id
				HAVING COUNT(TRIM(cst_id)) > 1
			) t
		);

		-- Step 2: Create Temp Table 2
		--- retrying to integrate in some dupes rather than drop them all according to the data input
		WITH dupe_select AS (
			SELECT 
				[cst_id],
				[cst_key],
				[cst_firstname],
				[cst_lastname],
				[cst_marital_status],
				[cst_gndr],
				[cst_create_date],
				-- Check for NOT NULL for each column
				CASE WHEN [cst_firstname] IS NOT NULL THEN 1 ELSE 0 END AS first_name_not_null,
				CASE WHEN [cst_lastname] IS NOT NULL THEN 1 ELSE 0 END AS last_name_not_null,
				CASE WHEN [cst_marital_status] IS NOT NULL THEN 1 ELSE 0 END AS marital_status_not_null,
				CASE WHEN [cst_gndr] IS NOT NULL THEN 1 ELSE 0 END AS gender_not_null,
				-- Compute total non-null count
				(CASE WHEN [cst_firstname] IS NOT NULL THEN 1 ELSE 0 END + 
					CASE WHEN [cst_lastname] IS NOT NULL THEN 1 ELSE 0 END + 
					CASE WHEN [cst_marital_status] IS NOT NULL THEN 1 ELSE 0 END + 
					CASE WHEN [cst_gndr] IS NOT NULL THEN 1 ELSE 0 END) AS total_null,
				-- Partitioning for most recent record 
				ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS recency
			FROM bronze.crm_cust_info
			WHERE [cst_id] IN ( 
				SELECT t.rep_cst_id 
				FROM (
					SELECT 
						TRIM([cst_id]) AS rep_cst_id, 
						COUNT(TRIM([cst_id])) AS n_count 
					FROM bronze.crm_cust_info 
					GROUP BY [cst_id]
					HAVING COUNT(TRIM([cst_id])) > 1
				) t
			)
		)
		SELECT 
			[cst_id],
			[cst_key],
			[cst_firstname],
			[cst_lastname],
			[cst_marital_status],
			[cst_gndr],
			[cst_create_date],
			GETDATE() AS Created_at 
		INTO #temp_table_2
		FROM dupe_select
		WHERE total_null >= 3 AND recency = 1;

		-- Step 3: Combine the results
		BEGIN TRY 
			INSERT INTO silver.crm_cust_info(
				[crm_cst_id],
				[crm_cst_key],
				[crm_cst_firstname],
				[crm_cst_lastname],
				[crm_cst_marital_status],
				[crm_cst_gender],
				[crm_cst_create_date],
				[dwh_created_at]
			)
			SELECT	[cst_id],
					[cst_key],
					[cst_firstname],
					[cst_lastname],
					[cst_marital_status],
					[cst_gndr],
					[cst_create_date],
					[Created_at]
			FROM (
						SELECT * FROM #temp_table_1
						UNION
						SELECT * FROM #temp_table_2
				) AS Combined_table;

			PRINT 'Created, Transformed and loaded successfully TABLE: silver.crm_cst_info'	
			
			PRINT '==============================================================================';
			
			PRINT ''
		END TRY
		BEGIN CATCH 
			PRINT 'Table data insertion ERROR: silver.crm_cst_info';
			PRINT 'ERROR:'+ ERROR_MESSAGE();
		END CATCH;
		DROP TABLE IF EXISTS #temp_table_1, #temp_table_2;
		PRINT '==============================================================================';


		-- silver.cust_info table end.


	
		-- erp_Loc table begin.

		PRINT 'TABLE: silver.erp_Loc, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		IF OBJECT_ID('silver.erp_Loc') IS NOT NULL
			BEGIN
				PRINT 'Dropping silver.erp_Loc table';
				DROP TABLE silver.erp_Loc;
				PRINT 'Dropped silver.erp_Loc table';
			END
			ELSE
				PRINT 'silver.erp_Loc table doesnot exist';
		PRINT 'Creating silver.erp_Loc table';

		CREATE TABLE silver.erp_Loc(
			[erp_loc_id] NVARCHAR(50),
			[erp_customer_id] INT,
			[erp_location_country] NVARCHAR(50),
			[dwh_created_at] DATETIME,
		);
		BEGIN TRY 
			INSERT INTO silver.erp_Loc(
				[erp_loc_id],
				[erp_customer_id],
				[erp_location_country],
				[dwh_created_at]
			)
			SELECT 
				REPLACE(loc_id,'-','') AS loc_id,
				CAST(SUBSTRING(loc_id,7,LEN(loc_id)) AS NUMERIC) AS cst_id,
				TRIM(CASE 
						WHEN [loc_cntry] = 'US' THEN 'United States'
						WHEN [loc_cntry] = 'USA' THEN 'United States'
						WHEN [loc_cntry] = '' THEN NULL
						WHEN [loc_cntry] = 'DE' THEN 'Germany'
						ELSE [loc_cntry]
					END) AS loc_cntry,
				GETDATE() as dwh_created_at
			FROM bronze.erp_Loc;
			PRINT 'Created, Transformed and loaded successfully TABLE: silver.erp_Loc';	
			PRINT '==============================================================================';
			PRINT '';
		END TRY
		BEGIN CATCH 
			PRINT 'Table data insertion ERROR: silver.erp_Loc';
			PRINT 'ERROR:'+ ERROR_MESSAGE();
		END CATCH;

		PRINT '==============================================================================';


		-- erp_Loc table end.
		


		-- erp_cust table begin.

		PRINT 'TABLE: silver.erp_cust, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		IF OBJECT_ID('silver.erp_cust') IS NOT NULL
			BEGIN
				PRINT 'Dropping silver.erp_cust table';
				DROP TABLE silver.erp_cust;
				PRINT 'Dropped silver.erp_cust table';
			END
			ELSE
				PRINT 'silver.erp_cust table doesnot exist';
		PRINT 'Creating silver.erp_cust table';
		CREATE TABLE silver.erp_cust(
				erp_customer_id NVARCHAR(100),
				erp_birth_date DATE,
				erp_gender NVARCHAR(50),
				erp_dwh_created_at DATETIME
				);
		PRINT 'Table has been created Successfully: silver.erp_Cust';
		-- insert silver layer data for table silver.crm_sales
		BEGIN TRY
			WITH clean_erp_cust AS(
				SELECT
					CAST(SUBSTRING(cust_id, 9,LEN(cust_id)) AS NVARCHAR) as customer_id,
					TRY_CONVERT(DATE, cust_Bdate) as birth_date,
					CASE
						WHEN UPPER(TRIM(cust_gender)) = 'F' THEN 'Female'
						WHEN UPPER(TRIM(cust_gender)) = 'M' THEN 'Male'
						WHEN UPPER(TRIM(cust_gender)) = '' THEN NULL
						ELSE cust_gender
					END AS gender,
					DATEDIFF(YEAR, TRY_CONVERT(DATE, cust_Bdate), GETDATE()) -
					CASE 
						WHEN MONTH(TRY_CONVERT(DATE, cust_Bdate)) > MONTH(GETDATE()) OR 
							(MONTH(TRY_CONVERT(DATE, cust_Bdate)) = MONTH(GETDATE()) AND 
							DAY(TRY_CONVERT(DATE, cust_Bdate)) > DAY(GETDATE()))
						THEN 1
						ELSE 0
					END AS age,

					ROW_NUMBER() OVER(
						PARTITION BY CAST(SUBSTRING(cust_id, 9, LEN(cust_id)) AS NVARCHAR) 
						ORDER BY cust_id
					) AS rn
				FROM  bronze.erp_cust
				),

				Final AS(
					SELECT 
						customer_id,
						birth_date,
						gender,
						GETDATE() AS dwh_created_at
					FROM clean_erp_cust  WHERE age BETWEEN 1 AND 109 AND rn = 1
				)
				INSERT INTO silver.erp_cust(
						erp_customer_id,
						erp_birth_date,
						erp_gender,
						erp_dwh_created_at
				)
				SELECT * FROM Final;
			PRINT 'Created, Transformed and loaded successfully TABLE: silver.erp_cust';
		END TRY
		BEGIN CATCH
			PRINT 'Table data insertion ERROR: silver.erp_cust';
			PRINT 'ERROR:'+ ERROR_MESSAGE();
		END CATCH;

		PRINT 'Data import successfully table: silver.erp_cust';
		PRINT '==============================================================================';

		PRINT '';

		PRINT '==============================================================================';

		-- erp_cust table end.


		-- erp_px_cat table begin.
		PRINT 'TABLE: silver.erp_px_cat, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		IF OBJECT_ID('silver.erp_px_cat') IS NOT NULL
			BEGIN
				PRINT 'Dropping silver.erp_px_cat table';
				DROP TABLE silver.erp_px_cat;
				PRINT 'Dropped silver.erp_px_cat table';
			END
			ELSE
				PRINT 'silver.erp_px_cat table doesnot exist';
		PRINT 'Creating silver.erp_px_cat table';
		CREATE TABLE silver.erp_px_cat(
			erp_cat_id NVARCHAR(100),
			erp_cat_cat NVARCHAR(225),
			erp_cat_subcat NVARCHAR(225),
			erp_cat_maintenace NVARCHAR(50),
			dwh_created_at DATETIME
		);

		BEGIN TRY
			WITH clean_cat AS (
				SELECT 
					CAST(REPLACE(TRIM(UPPER(cat_id)), '_', '-') AS NVARCHAR) AS cat_id,
					TRIM(cat_cat) AS cat_cat,
					TRIM(cat_subcat) AS cat_subcat,
					TRIM(cat_maintenace) AS cat_maintenace
				FROM bronze.erp_px_cat),
				Final AS (
					SELECT 
						cat_Id,
						cat_cat,
						cat_subcat,
						cat_maintenace,
						GETDATE() as dw_created_at
					FROM clean_cat
				)
				INSERT INTO silver.erp_px_cat(
					erp_cat_id,
					erp_cat_cat,
					erp_cat_subcat,
					erp_cat_maintenace,
					dwh_created_at
				)
				SELECT * FROM Final;
		END TRY
		BEGIN CATCH 
			PRINT 'Table data insertion ERROR: silver.erp_px_cat';
			PRINT 'ERROR:'+ ERROR_MESSAGE()
		END CATCH;
		
	PRINT '==============================================================================';

	-- erp_px_cat table end.

	-- overall end time setup
	SET @end_time = GETDATE();

	PRINT '';
	PRINT '';
	PRINT '==============================================================================';
	PRINT '==============================================================================';
	PRINT 'LAYER: SILVER LOADING TIME (seconds)';
	PRINT 'TIME TAKEN: ' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
	PRINT '==============================================================================';
	PRINT '==============================================================================';
	PRINT '';
	PRINT '';

	END;


USE DWH;

GO

CREATE OR ALTER PROCEDURE Load_Bronze_Layer AS 
	BEGIN
		-- Switching to the correct database 
		PRINT 'SWITCHING TO DWH';

		PRINT '';

		PRINT '==============================================================================';
		PRINT '                           BRONZE LAYER STARTING                              ';
		PRINT '==============================================================================';
		PRINT '';

		PRINT '==============================================================================';
		PRINT 'TABLE: Bronze.crm_sales, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';


		-- overall time set up 
		DECLARE @start_time DATETIME, @end_time DATETIME;

		-- crm_sales table begin.

		-- set overrall start time
		SET @start_time = GETDATE();

		IF OBJECT_ID('bronze.crm_sales') IS NOT NULL
			BEGIN
				PRINT 'Dropping bronze.crm_Sales table';
				DROP TABLE bronze.crm_sales;
				PRINT 'Dropped crm_sales';
			END 
		ELSE
			PRINT 'crm_sales does not exist';

		PRINT 'Creating Bronze.crm_sales table';
		CREATE TABLE bronze.crm_sales(
			sls_ord_num NVARCHAR(50),
			sls_prd_key NVARCHAR(50),
			sls_cust_id	NVARCHAR(50),
			sls_order_dt NVARCHAR(50),
			sls_ship_dt	NVARCHAR(50),
			sls_due_dt NVARCHAR(50),
			sls_sales INT,
			sls_quantity INT,	
			sls_price INT
		);
		PRINT 'Table has been created Successfully: Bronze.crm_sales';	
		PRINT '==============================================================================';

		PRINT '';

		PRINT '==============================================================================';
		PRINT 'TABLE: Bronze.crm_prd_info, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		-- crm_sales table end

		-- prd_info table begin.
		IF OBJECT_ID('bronze.crm_prd_info') IS NOT NULL
			BEGIN
				PRINT 'Dropping Bronze.crm_prd_info table';
				DROP TABLE bronze.crm_prd_info;
				PRINT 'Dropped Bronze.crm_prd_info table';
			END
			ELSE
				PRINT 'Bronze.crm_prd_info table doesnot exist';
		PRINT 'Creating Bronze.crm_prd_info table';
		CREATE TABLE bronze.crm_prd_info(
			prd_id INT,
			prd_key NVARCHAR(50),
			prd_nm NVARCHAR(80),
			prd_cost INT,
			prd_line NVARCHAR(50),
			prd_start_dt DATE,
			prd_end_dt DATE
			);
		PRINT 'Table has been created Successfully: Bronze.crm_prd_info';	
		PRINT '==============================================================================';

		PRINT '';
		-- prd_info table end.


		PRINT '==============================================================================';
		PRINT 'TABLE: Bronze.crm_cust_info, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		-- cust_info table begin.
		IF OBJECT_ID('bronze.crm_cust_info') IS NOT NULL
			BEGIN
				PRINT 'Dropping Bronze.crm_cust_info table';
				DROP TABLE bronze.crm_cust_info;
				PRINT 'Dropped Bronze.crm_cust_info table';
			END
			ELSE
				PRINT 'Bronze.crm_cust_info table doesnot exist';
		PRINT 'Creating Bronze.crm_cust_info table';
		CREATE TABLE bronze.crm_cust_info(
			cst_id NVARCHAR(50),
			cst_key NVARCHAR(50),
			cst_firstname NVARCHAR(50),
			cst_lastname NVARCHAR(50),
			cst_marital_status NVARCHAR(5),
			cst_gndr NVARCHAR(5),
			cst_create_date DATE

			);
		PRINT 'Table has been created Successfully: Bronze.crm_cust_info';	
		PRINT '==============================================================================';

		PRINT '';
		-- cust_info table end.

		PRINT '==============================================================================';
		PRINT 'TABLE: Bronze.erp_px_cat, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		-- erp_px_cat table begin.
		IF OBJECT_ID('bronze.erp_px_cat') IS NOT NULL
			BEGIN
				PRINT 'Dropping Bronze.erp_px_cat table';
				DROP TABLE bronze.erp_px_cat;
				PRINT 'Dropped Bronze.erp_px_cat table';
			END
			ELSE
				PRINT 'Bronze.erp_px_cat table doesnot exist';
		PRINT 'Creating Bronze.erp_px_cat table';
		CREATE TABLE bronze.erp_px_cat(
			cat_id NVARCHAR(50),
			cat_cat NVARCHAR(50),
			cat_subcat NVARCHAR(50),
			cat_maintenace NVARCHAR(50)
		);
		PRINT 'Table has been created Successfully: Bronze.erp_px_cat';	
		PRINT '==============================================================================';

		PRINT '';
		-- erp_px_cat table end.

		PRINT '==============================================================================';
		PRINT 'TABLE: Bronze.erp_Loc, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		-- erp_Loc table begin.
		IF OBJECT_ID('bronze.erp_Loc') IS NOT NULL
			BEGIN
				PRINT 'Dropping Bronze.erp_Loc table';
				DROP TABLE bronze.erp_Loc;
				PRINT 'Dropped Bronze.erp_Loc table';
			END
			ELSE
				PRINT 'Bronze.erp_Loc table doesnot exist';
		PRINT 'Creating Bronze.erp_Loc table';
		CREATE TABLE bronze.erp_Loc(
			loc_id NVARCHAR(50),
			loc_cntry NVARCHAR(50)
		);
		PRINT 'Table has been created Successfully: Bronze.erp_Loc';	
		PRINT '==============================================================================';

		PRINT '';
		-- erp_Loc table end.

		PRINT '==============================================================================';
		PRINT 'TABLE: Bronze.erp_cust, ACTION: Creation/setup';
		PRINT '------------------------------------------------------------------------------';

		-- erp_cust table begin.
		IF OBJECT_ID('bronze.erp_cust') IS NOT NULL
			BEGIN
				PRINT 'Dropping Bronze.erp_cust table';
				DROP TABLE bronze.erp_cust;
				PRINT 'Dropped Bronze.erp_cust table';
			END
			ELSE
				PRINT 'Bronze.erp_cust table doesnot exist';
		PRINT 'Creating Bronze.erp_cust table';
		CREATE TABLE bronze.erp_cust(
			cust_id NVARCHAR(50),
			cust_Bdate DATE,
			cust_gender NVARCHAR(50)
		);
		PRINT 'Table has been created Successfully: Bronze.erp_Cust';	
		PRINT '==============================================================================';

		PRINT '';
		-- erp_cust table end.

		----------------------------------------------------------------------------------------------------------------------------------------

		----------------------------------------------------------------------------------------------------------------------------------------
		-- LOADING IN DATA INTO TABLES 

		PRINT '==============================================================================';
		PRINT 'ATTEMPTING TO IMPORT DATA INTO BRONZE LAYER';
		PRINT '==============================================================================';
		PRINT '';
		-- load crm_sales begin
		PRINT '==============================================================================';
		PRINT 'TABLE: bronze.crm_sales , ACTION: Data import';

		-- tbl time setup 
		DECLARE @tbl_start_time DATETIME, @tbl_end_time DATETIME;

		BEGIN TRY

			-- table  start time setup 
			SET @tbl_start_time = GETDATE();

			BULK INSERT bronze.crm_sales 
			FROM 'C:\Users\M D\Downloads\Data_source\soucre_crm\sales_details.csv' -- replace with path to your file location
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);	

			-- table end time setup
			SET @tbl_end_time = GETDATE();
			PRINT '------------------------------------------------------------------------------';
			PRINT 'Data imported successfully table: Bronze.crm_sales';	
			PRINT 'LAYER: BRONZE';
			PRINT 'TABLE: crm_sales';
			PRINT 'LOADING TIME (seconds): ' +  ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
			PRINT '==============================================================================';
			PRINT '';
		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: crm_sales' + ERROR_MESSAGE();
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
		

		-- load crm_sales End.

		-- load crm_prd_info begin

		PRINT '==============================================================================';

		PRINT 'TABLE: bronze.prd_info , ACTION: Data import';
		BEGIN TRY

			-- table time set up 
			SET @tbl_start_time = GETDATE();

			BULK INSERT bronze.crm_prd_info 
			FROM 'C:\Users\M D\Downloads\Data_source\soucre_crm\prd_info.csv' -- replace with path to your file location
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
			PRINT '------------------------------------------------------------------------------';

			PRINT 'Data imported successfully table: Bronze.crm_prd_info';

			-- table end time setup
			SET @tbl_end_time = GETDATE();
			PRINT 'LAYER: BRONZE';
			PRINT 'TABLE: crm_prd_info';
			PRINT 'LOADING TIME (seconds): ' +  ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
			PRINT '==============================================================================';
			PRINT '';
			
			-- load crm_prd_info End.
		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: crm_prd_info' + ERROR_MESSAGE();
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
		



		-- load crm_cust_info begin

		PRINT '==============================================================================';
		PRINT 'TABLE: bronze.crm_cust_info , ACTION: Data import';
		BEGIN TRY

			-- table time set up 
			SET @tbl_start_time = GETDATE();


			BULK INSERT bronze.crm_cust_info 
			FROM 'C:\Users\M D\Downloads\Data_source\soucre_crm\cust_info.csv' -- replace with path to your file location
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
	
			-- table end time setup
			SET @tbl_end_time = GETDATE();
			PRINT '------------------------------------------------------------------------------';
			PRINT 'Data imported successfully table: Bronze.cust_info';	
			PRINT 'LAYER: BRONZE';
			PRINT 'TABLE: cust_info';
			PRINT 'LOADING TIME (seconds): ' +  ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
			PRINT '==============================================================================';
			PRINT '';

			-- load crm_cust_info End.

		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: crm_cust_info' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	

		-- load erp_px_cat begin

		PRINT '==============================================================================';
		PRINT 'TABLE: bronze.erp_px_cat , ACTION: Data import';
		BEGIN TRY

			-- table time set up 
			SET @tbl_start_time = GETDATE();

			BULK INSERT bronze.erp_px_cat 
			FROM 'C:\Users\M D\Downloads\Data_source\source_erp\PX_CAT_G1V2.csv' -- replace with path to your file location
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
		
			-- table end time setup
			SET @tbl_end_time = GETDATE();

			PRINT '------------------------------------------------------------------------------';

			PRINT 'Data imported successfully table: Bronze.erp_px_cat';	
			PRINT 'LAYER: BRONZE';
			PRINT 'TABLE: erp_px_cat';
			PRINT 'LOADING TIME (seconds): ' +  ' '  + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
			PRINT '==============================================================================';
			PRINT '';
			-- load erp_px_cat End.

		END TRY
		BEGIN CATCH
			PRINT 'An Error occured While Trying to insert Data in Table: erp_px_cat' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	
	
		-- load erp_Loc begin

		PRINT '==============================================================================';
		PRINT 'TABLE: bronze.erp_Loc , ACTION: Data import';
		BEGIN TRY 

			-- table time set up 
			SET @tbl_start_time = GETDATE();


			BULK INSERT bronze.erp_Loc 
			FROM 'C:\Users\M D\Downloads\Data_source\source_erp\LOC_A101.csv' -- replace with path to your file location
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
	
			-- table end time setup
			SET @tbl_end_time = GETDATE();

			PRINT '------------------------------------------------------------------------------';
			PRINT 'Data imported successfully table: Bronze.erp_Loc';	
			PRINT 'LAYER: BRONZE';
			PRINT 'TABLE: erp_Loc';
			PRINT 'LOADING TIME (seconds): ' + ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
			PRINT '==============================================================================';
			PRINT '';

			-- load erp_Loc End.

		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: erp_Loc' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	

		-- load erp_cust begin

		PRINT '==============================================================================';
		PRINT 'TABLE: bronze.erp_Cust , ACTION: Data import';
		BEGIN TRY 

			-- table time set up 
			SET @tbl_start_time = GETDATE();

			BULK INSERT bronze.erp_cust
			FROM 'C:\Users\M D\Downloads\Data_source\source_erp\CUST_AZ12.csv' -- replace with path to your file location
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);

			-- table end time setup
			SET @tbl_end_time = GETDATE();

			PRINT '------------------------------------------------------------------------------';

			PRINT 'Data imported successfully table: Bronze.erp_Cust';	
			PRINT 'LAYER: BRONZE';
			PRINT 'TABLE: erp_Cust';
			PRINT 'LOADING TIME (seconds): ' + ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
			PRINT '==============================================================================';
			PRINT '';

			-- load erp_cust End.

		END TRY
		BEGIN CATCH
			PRINT 'An Error occured While Trying to insert Data in Table: erp_cust' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;

		--end time 
		SET @end_time = GETDATE();
	
	PRINT '';
	PRINT '';
	PRINT '==============================================================================';
	PRINT '==============================================================================';
	PRINT 'LAYER: BRONZE LOADING TIME (seconds)';
	PRINT 'TIME TAKEN: ' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
	PRINT '==============================================================================';
	PRINT '==============================================================================';
	PRINT '';
	PRINT '';
	
	END;


CREATE OR ALTER PROCEDURE Load_Bronze_Layer AS 
	BEGIN
		-- Switching to the correct database 
		PRINT 'SWITCHING TO DWH';
		PRINT '';
		PRINT '===============================================================';
		PRINT 'BEGINNING TO TRUNCATE AND LOAD DATA ';
		PRINT '===============================================================';
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: Bronze.crm_sales, ACTION: Creation/setup';
		PRINT '---------------------------------------------------------------';

		-- crm_sales table begin.
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

		PRINT 'Data import successfully table: Bronze.crm_sales';
		PRINT '---------------------------------------------------------------';
		PRINT '';
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: Bronze.crm_prd_info, ACTION: Creation/setup';
		PRINT '---------------------------------------------------------------';
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
		PRINT '---------------------------------------------------------------';
		PRINT '';
		-- prd_info table end.

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
		PRINT '---------------------------------------------------------------';
		PRINT '';
		-- cust_info table end.

	
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
		PRINT '---------------------------------------------------------------';
		PRINT '';
		-- erp_px_cat table end.


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
		PRINT '---------------------------------------------------------------';
		PRINT '';
		-- erp_Loc table end.


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
		PRINT '---------------------------------------------------------------';
		PRINT '';
		-- erp_cust table end.

		----------------------------------------------------------------------------------------------------------------------------------------

		----------------------------------------------------------------------------------------------------------------------------------------
		-- LOADING IN DATA INTO TABLES 
		PRINT '===============================================================';
		PRINT 'ATTEMPTING TO IMPORT DATA INTO BRONZE LAYER';
		PRINT '===============================================================';
		-- load crm_sales begin
		PRINT 'TABLE: bronze.crm_sales , ACTION: Data import';
		BEGIN TRY 
			BULK INSERT bronze.crm_sales 
			FROM 'C:\Users\M D\Downloads\Data_source\soucre_crm\sales_details.csv'
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);	
	
			PRINT 'Data import successfully table: Bronze.crm_sales';	
			PRINT '---------------------------------------------------------------';
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
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: bronze.prd_info , ACTION: Data import';
		BEGIN TRY
			BULK INSERT bronze.crm_prd_info 
			FROM 'C:\Users\M D\Downloads\Data_source\soucre_crm\prd_info.csv'
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
			PRINT 'Data import successfully table: Bronze.crm_prd_info';	
			PRINT '---------------------------------------------------------------';
			PRINT '';
			PRINT '---------------------------------------------------------------';
			-- load crm_prd_info End.
		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: crm_prd_info' + ERROR_MESSAGE();
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
		



			-- load crm_cust_info begin
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: bronze.crm_cust_info , ACTION: Data import';
		BEGIN TRY
			BULK INSERT bronze.crm_cust_info 
			FROM 'C:\Users\M D\Downloads\Data_source\soucre_crm\cust_info.csv'
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
	
			PRINT 'Data import successfully table: Bronze.crm_cust_info';	
			PRINT '---------------------------------------------------------------';
			PRINT '';
			-- load crm_cust_info End.
			PRINT '---------------------------------------------------------------';
		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: crm_cust_info' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	

		-- load erp_px_cat begin
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: bronze.erp_px_cat , ACTION: Data import';
		BEGIN TRY
			BULK INSERT bronze.erp_px_cat 
			FROM 'C:\Users\M D\Downloads\Data_source\source_erp\PX_CAT_G1V2.csv'
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
		
			PRINT 'Data import successfully table: Bronze.erp_px_cat';	
			PRINT '---------------------------------------------------------------';
			PRINT '';
			-- load erp_px_cat End.
			PRINT '---------------------------------------------------------------';
		END TRY
		BEGIN CATCH
			PRINT 'An Error occured While Trying to insert Data in Table: erp_px_cat' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	
	
		-- load erp_Loc begin
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: bronze.erp_Loc , ACTION: Data import';
		BEGIN TRY 
			BULK INSERT bronze.erp_Loc 
			FROM 'C:\Users\M D\Downloads\Data_source\source_erp\LOC_A101.csv'
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
	
			PRINT 'Data import successfully table: Bronze.erp_Loc';	
			PRINT '---------------------------------------------------------------';
			PRINT '';
			-- load erp_Loc End.
			PRINT '---------------------------------------------------------------';
		END TRY
		BEGIN CATCH 
			PRINT 'An Error occured While Trying to insert Data in Table: erp_Loc' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	

		-- load erp_cust begin
		PRINT '---------------------------------------------------------------';
		PRINT 'TABLE: bronze.erp_Cust , ACTION: Data import';
		BEGIN TRY 
			BULK INSERT bronze.erp_cust
			FROM 'C:\Users\M D\Downloads\Data_source\source_erp\CUST_AZ12.csv'
			WITH (
				FIELDTERMINATOR = ',',
				ROWTERMINATOR = '\n',  -- or '0x0D0A' or '0x0a' if needed
				FIRSTROW = 2,
				TABLOCK
			);
			PRINT 'Data import successfully table: Bronze.erp_cust';	
			PRINT '---------------------------------------------------------------';
			PRINT '';
			-- load erp_cust End.
			PRINT '---------------------------------------------------------------';
		END TRY
		BEGIN CATCH
			PRINT 'An Error occured While Trying to insert Data in Table: erp_cust' + ERROR_MESSAGE(); 
			PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
			PRINT 'Error Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR(10));
			PRINT 'Error State: ' + CAST(ERROR_STATE() AS VARCHAR(10));
		END CATCH;
	
	END;

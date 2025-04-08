/*
     This scripts creates the Gold layer as views of the other refined tables creating the tables below.
          Tables:
               Products
               customers

*/
CREATE OR ALTER PROCEDURE Load_gold_Layer AS
     BEGIN
     -- TIME SETUP 
     PRINT '';
     PRINT '';
	PRINT '==============================================================================';
	PRINT '                           GOLD LAYER STARTING                               ';
	PRINT '==============================================================================';
     PRINT '';


	DECLARE @start_time DATETIME, @end_time DATETIME, @tbl_start_time DATETIME, @tbl_end_time DATETIME;

	-- overall start time setup 
	SET @start_time = GETDATE();

     -- Drop the view for all products sold if it exists
     IF OBJECT_ID('gold.all_products_sold', 'V') IS NOT NULL
          EXEC('DROP VIEW gold.all_products_sold');

     -- Create the view for all products sold

	SET @tbl_start_time = GETDATE();

     EXEC('
          CREATE OR ALTER VIEW gold.all_products_sold AS
          SELECT
               crm_prd.crm_prd_Cat_id AS category_id,
               crm_prd.crm_prd_key AS product_key,
               crm_prd.crm_prd_name AS product_name,
               crm_prd.crm_prd_cost AS product_cost,
               crm_prd.crm_prd_line AS product_line,
               crm_prd.crm_prd_start_date AS product_start_date,
               crm_prd.crm_prd_end_date AS product_end_date,
               erp_px.erp_cat_maintenace AS category_maintenace,
               erp_px.erp_cat_cat AS category,
               erp_px.erp_cat_subcat AS sub_category
          FROM silver.crm_prd_info AS crm_prd
          LEFT JOIN silver.erp_px_cat AS erp_px 
               ON crm_prd.crm_prd_cat_id = erp_px.erp_cat_id;
     ');

               
     SET @tbl_end_time = GETDATE();
     PRINT '==============================================================================';
     PRINT 'Data Transform successfully View: GOLD.all_products_sold';	
	PRINT '------------------------------------------------------------------------------';
     PRINT 'LAYER: GOLD';
     PRINT 'VIEW: all_products_sold';
     PRINT 'LOADING TIME (seconds)' + ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
     PRINT '==============================================================================';
     PRINT '';


     -- Drop the view for current products sold if it exists
     IF OBJECT_ID('gold.current_products_sold', 'V') IS NOT NULL
          EXEC('DROP VIEW gold.current_products_sold');

     
	SET @tbl_start_time = GETDATE();
     -- Create the view for current products sold
     EXEC('
          CREATE OR ALTER VIEW gold.current_products_sold AS
          SELECT
               crm_prd.crm_prd_Cat_id AS category_id,
               crm_prd.crm_prd_key AS product_key,
               crm_prd.crm_prd_name AS product_name,
               crm_prd.crm_prd_cost AS product_cost,
               crm_prd.crm_prd_line AS product_line,
               crm_prd.crm_prd_start_date AS product_start_date,
               crm_prd.crm_prd_end_date AS product_end_date,
               erp_px.erp_cat_maintenace AS category_maintenace,
               erp_px.erp_cat_cat AS category,
               erp_px.erp_cat_subcat AS sub_category
          FROM silver.crm_prd_info AS crm_prd
          LEFT JOIN silver.erp_px_cat AS erp_px 
               ON crm_prd.crm_prd_cat_id = erp_px.erp_cat_id
          WHERE crm_prd.crm_prd_end_date IS NULL;
     ');

     SET @tbl_end_time = GETDATE();
     PRINT '==============================================================================';
     PRINT 'Data Transform successfully View: GOLD.current_products_sold';	
	PRINT '------------------------------------------------------------------------------';
     PRINT 'LAYER: GOLD';
     PRINT 'VIEW: current_products_sold';
     PRINT 'LOADING TIME (seconds)' + ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
     PRINT '==============================================================================';
     PRINT '';

     -- Drop the view for all customer_sales if it exists
     IF OBJECT_ID('gold.customer_sales', 'V') IS NOT NULL
          EXEC('DROP VIEW gold.customer_sales');

	SET @tbl_start_time = GETDATE();
     -- Create the view for current products sold
     EXEC('
          CREATE OR ALTER VIEW gold.customer_sales AS
          WITH clean_cust_info AS (
               SELECT 
                    erp_customer_id AS customer_id,
                    crm_cst_firstname AS first_name,
                    crm_cst_lastname AS last_name,
                    crm_cst_marital_status AS marital_status,
                    CASE
                         WHEN erp_gender IS NOT  NULL AND crm_cst_gender IS NOT NULL  AND  erp_gender != crm_cst_gender
                              THEN crm_cst_gender
                         WHEN crm_cst_gender IS NULL AND erp_gender IS NOT NULL THEN erp_gender
                         ELSE
                              crm_cst_gender
                    END AS gender,
                    erp_birth_date AS customer_birthday,
                    crm_cst_create_date AS joined_on
               FROM silver.[erp_cust] AS erp_cust LEFT JOIN silver.[crm_cust_info] AS crm_cust
               ON  erp_cust.erp_customer_id = crm_cust.crm_cst_id
               ),

               Sales AS(
                    SELECT * FROM silver.crm_sales
               ),

               Final AS(
                    SELECT 
                         sales.crm_order_id AS order_id,
                         sales.crm_product_key AS product_key,
                         sales.crm_customer_id AS customer_id,
                         sales.crm_order_date AS order_date,
                         sales.crm_ship_date AS ship_date,
                         sales.crm_due_date AS due_date,
                         sales.crm_price AS price,
                         sales.crm_quantity AS quantity,
                         sales.crm_revenue AS revenue,
                         COALESCE(clean_cust_info.first_name, ''N/A'') AS first_name,
                         COALESCE(clean_cust_info.last_name, ''N/A'') AS last_name	,
                         COALESCE(clean_cust_info.marital_status, ''Unknown'') AS marital_status,
                         COALESCE(clean_cust_info.gender, ''Unknown'')	AS gender ,
                         COALESCE(CAST(clean_cust_info.customer_birthday AS VARCHAR), ''Unknown'') AS customer_birthday,
                         COALESCE(loc.erp_location_country, ''Unknown'') AS country,
                         CURRENT_TIMESTAMP AS run_on_date,
                         ROW_NUMBER() OVER(PARTITION BY sales.crm_order_date, 
                              sales.crm_order_id ORDER BY sales.crm_order_date
                              ) AS Row_num
                    FROM Sales LEFT JOIN clean_cust_info
                    ON sales.crm_customer_id = clean_cust_info.customer_id
                    LEFT JOIN silver.erp_Loc as loc
                    ON loc.erp_customer_id = clean_cust_info.customer_id
                    WHERE COALESCE(sales.crm_order_date, ''1900-01-01'') != ''1900-01-01''

               )
               
               SELECT 
                    order_id,
                    product_key,
                    customer_id,
                    order_date,
                    ship_date,
                    due_date,
                    price,
                    quantity,
                    revenue,
                    first_name,
                    last_name,
                    marital_status,
                    gender,
                    customer_birthday,
                    country,
                    run_on_date
               FROM Final;
     ');
     
     SET @tbl_end_time = GETDATE();
     PRINT '==============================================================================';
     PRINT 'Data Transform successfully table: GOLD.customer_sales';	
	PRINT '------------------------------------------------------------------------------';
     PRINT 'LAYER: GOLD';
     PRINT 'VIEW: customer_sales';
     PRINT 'LOADING TIME (seconds)' + ' ' + CAST(DATEDIFF(SECOND, @tbl_start_time, @tbl_end_time) AS NVARCHAR);
     PRINT '==============================================================================';
     PRINT '';

     -- overall end time setup
	SET @end_time = GETDATE();

	PRINT '';
	PRINT '==============================================================================';
	PRINT '==============================================================================';
	PRINT '';
     PRINT 'LAYER: GOLD LOADING TIME (seconds)';
	PRINT 'TIME TAKEN: ' + ' ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR)
	PRINT '==============================================================================';
	PRINT '==============================================================================';
	PRINT '';
	PRINT '';

     END;

/*
     This scripts creates the Gold layer as views of the other refined tables creating the tables below.
          Tables:
               Products
               customers

*/
CREATE OR ALTER PROCEDURE Load_gold_Layer AS
     BEGIN
     -- Drop the view for all products sold if it exists
     IF OBJECT_ID('gold.all_products_sold', 'V') IS NOT NULL
          EXEC('DROP VIEW gold.all_products_sold');

     -- Create the view for all products sold
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
               ON crm_prd.crm_prd_cat_id = erp_px.erp_cat_id
     ');

     -- Drop the view for current products sold if it exists
     IF OBJECT_ID('gold.current_products_sold', 'V') IS NOT NULL
          EXEC('DROP VIEW gold.current_products_sold');

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
          WHERE crm_prd.crm_prd_end_date IS NULL
     ');
     END;

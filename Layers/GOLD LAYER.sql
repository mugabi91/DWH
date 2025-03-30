/*
     This scripts creates the Gold layer as views of the other refined tables creating the tables below.
          Tables:
               Products
               customers

*/

CREATE OR ALTER VIEW all_products_sold AS
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
     FROM silver.crm_prd_info as crm_prd LEFT 
     JOIN silver.erp_px_cat as erp_px 
     ON crm_prd.crm_prd_cat_id = erp_px.erp_cat_id;

GO

CREATE OR ALTER VIEW current_products_sold AS
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
     FROM silver.crm_prd_info as crm_prd LEFT 
     JOIN silver.erp_px_cat as erp_px 
     ON crm_prd.crm_prd_cat_id = erp_px.erp_cat_id
     WHERE crm_prd.crm_prd_end_date IS NULL;
     
GO
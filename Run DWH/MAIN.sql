/*
    This script is used to load the data into the bronze layer and silver layer of the Data Warehouse.

*/
USE DWH;
    GO
    BEGIN TRY
        -- Create bronze layer, load data into bronze layer
        EXECUTE [Load_bronze_Layer]
    END TRY 
    BEGIN CATCH 
        PRINT 'Error in Load_bronze_Layer';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;

    GO

    BEGIN TRY 
        -- create silver layer, transform and load data into silver layer
        EXECUTE [Load_silver_Layer]
    END TRY    
    BEGIN CATCH 
        PRINT 'Error in Load_silver_Layer';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;

    GO

    BEGIN TRY 
        -- create silver layer, transform and load data into silver layer
        EXECUTE [Load_gold_Layer]
    END TRY    
    BEGIN CATCH 
        PRINT 'Error in Load_gold_Layer';
        PRINT ERROR_MESSAGE();
        RETURN;
    END CATCH;


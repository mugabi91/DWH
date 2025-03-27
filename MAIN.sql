USE DWH;

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
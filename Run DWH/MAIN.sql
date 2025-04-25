/*
    This script is used to load the data into the bronze layer and silver layer of the Data Warehouse.

*/
USE DWH;

GO

DECLARE @start_time_full DATETIME , @end_time_full DATETIME;
SET @start_time_full = GETDATE()

BEGIN TRY
    -- Create bronze layer, load data into bronze layer
    EXECUTE [Load_bronze_Layer]
    
    PRINT 'LAYER: Bronze created and Loaded successfully'

    PRINT '';
END TRY 
BEGIN CATCH 
    PRINT 'Error in Load_bronze_Layer';
    PRINT ERROR_MESSAGE();
    RETURN;
END CATCH;



BEGIN TRY 
    -- create silver layer, transform and load data into silver layer
    EXECUTE [Load_silver_Layer]
    PRINT 'LAYER: Silver created and Loaded successfully'
    PRINT '';
END TRY    
BEGIN CATCH 
    PRINT 'Error in Load_silver_Layer';
    PRINT ERROR_MESSAGE();
    RETURN;
END CATCH;



BEGIN TRY 
    -- create silver layer, transform and load data into silver layer
    EXECUTE [Load_gold_Layer]
    PRINT 'LAYER: Gold created and Loaded successfully'
END TRY    
BEGIN CATCH 
    PRINT 'Error in Load_gold_Layer';
    PRINT ERROR_MESSAGE();
    RETURN;
END CATCH;

SET @end_time_full = GETDATE();

PRINT ''
PRINT '==============================================================================';
PRINT '==============================================================================';

PRINT '';
PRINT 'SUCCESSFULLY CREATED AND LOADED ALL LAYERS IN DWH';
PRINT 'TOTAL TIME TAKEN:'+ ' ' + CAST(DATEDIFF(SECOND,@start_time_full, @end_time_full) AS NVARCHAR)
PRINT '==============================================================================';
PRINT '==============================================================================';

PRINT '';
PRINT '************************************ FIN **************************************';

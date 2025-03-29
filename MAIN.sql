USE master;
    GO
    -- Check if the database does NOT exist
    IF DB_ID('DWH') IS NULL
        BEGIN
            PRINT 'Database DWH does not exist.';
            PRINT 'Creating Database DWH...';
            CREATE DATABASE DWH;
            PRINT 'Database DWH created successfully.';
        END
        ELSE
        BEGIN
            PRINT 'Database DWH already exists. No action taken.';
        END

    GO

USE DWH;

    GO
    -- create the schemas layers if they do not exist
    IF SCHEMA_ID('bronze') IS NULL
        BEGIN
            PRINT 'Schema bronze does not exist.';
            PRINT 'Creating Schema bronze...';
            EXEC('CREATE SCHEMA bronze');
            PRINT 'Schema bronze created successfully.';
        END
        ELSE
        BEGIN
            PRINT 'Schema bronze already exists. No action taken.';
        END

        GO

    IF SCHEMA_ID('silver') IS NULL
        BEGIN
            PRINT 'Schema silver does not exist.';
            PRINT 'Creating Schema silver...';
            EXEC('CREATE SCHEMA silver');
            PRINT 'Schema silver created successfully.';
        END
        ELSE
        BEGIN
            PRINT 'Schema silver already exists. No action taken.';
        END

        GO

    IF SCHEMA_ID('gold') IS NULL
        BEGIN
            PRINT 'Schema gold does not exist.';
            PRINT 'Creating Schema gold...';
            EXEC('CREATE SCHEMA gold');
            PRINT 'Schema gold created successfully.';
        END
        ELSE
        BEGIN
            PRINT 'Schema gold already exists. No action taken.';
        END

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
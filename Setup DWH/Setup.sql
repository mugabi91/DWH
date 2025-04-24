/*
    This script sets up the DWH database and its schemas.
    It checks if the database and schemas exist before creating them.

*/

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
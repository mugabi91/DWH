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
        END;
    GO

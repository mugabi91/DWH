/*
    This script is used to drop the DWH database in SQL Server.
    It checks if the database exists and if so, sets it to SINGLE_USER mode
*/

USE master;
    GO
    
    If DB_ID('DWH') IS NOT NULL
        -- Set database to SINGLE_USER mode to forcefully disconnect all users
        BEGIN
            BEGIN TRY
                    ALTER DATABASE DWH SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                END TRY
                BEGIN CATCH
                    PRINT 'Error setting database to SINGLE_USER mode. It may not exist.';
                END CATCH
        
                -- Now drop the database
                BEGIN TRY
                    DROP DATABASE DWH;
                    PRINT 'Database DWH dropped successfully.';
                END TRY
                BEGIN CATCH
                    PRINT 'Error dropping database DWH. It may not exist.';
            END CATCH;
        END

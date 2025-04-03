CREATE OR ALTER PROCEDURE [dbo].[ExtractAndRemoveDuplicates]
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    
    -- This table will ensure consistent results structure
    DECLARE @Result TABLE (
        Success BIT,
        DuplicatesFound INT,
        RemainingRecords INT,
        ErrorNumber INT,
        ErrorMessage NVARCHAR(4000),
        ErrorLine INT
    );
    
    BEGIN TRY
        -- Check if source table exists
        IF NOT EXISTS (SELECT 1 FROM sys.tables WHERE name = 'csv_data' AND schema_id = SCHEMA_ID('dbo'))
        BEGIN
            -- Return error structure if table doesn't exist
            INSERT INTO @Result VALUES (
                0,        -- Success line
                0,        -- DuplicatesFound line
                0,        -- RemainingRecords line
                50001,    -- Custom error number line
                'Source table dbo.csv_data does not exist',
                0         -- Error line
            );
            SELECT * FROM @Result;
            RETURN;
        END
        
        BEGIN TRANSACTION;
        
        -- Create/truncate duplicates table
        IF OBJECT_ID('dbo.duplicates') IS NULL
        BEGIN
            CREATE TABLE dbo.duplicates (
                id INT,
                tpep_pickup_datetime DATETIME,
                tpep_dropoff_datetime DATETIME,
                passenger_count INT CHECK (passenger_count >= 0),
                trip_distance DECIMAL(10,2) CHECK (trip_distance >= 0),
                store_and_fwd_flag VARCHAR(3),
                pu_location_id INT,
                do_location_id INT,
                fare_amount DECIMAL(10,2),
                tip_amount DECIMAL(10,2)
            );
        END
        ELSE
        BEGIN
            TRUNCATE TABLE dbo.duplicates;
        END
        
        -- Insert duplicates
        INSERT INTO dbo.duplicates (
            id, tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count,
            trip_distance, store_and_fwd_flag, pu_location_id, do_location_id,
            fare_amount, tip_amount
        )
        SELECT 
            d.id, d.tpep_pickup_datetime, d.tpep_dropoff_datetime, d.passenger_count,
            d.trip_distance, d.store_and_fwd_flag, d.pu_location_id, d.do_location_id,
            d.fare_amount, d.tip_amount
        FROM dbo.csv_data d
        WHERE EXISTS (
            SELECT 1 
            FROM dbo.csv_data d2
            WHERE 
                (d2.tpep_pickup_datetime = d.tpep_pickup_datetime OR 
                (d2.tpep_pickup_datetime IS NULL AND d.tpep_pickup_datetime IS NULL))
              AND (d2.tpep_dropoff_datetime = d.tpep_dropoff_datetime OR 
                (d2.tpep_dropoff_datetime IS NULL AND d.tpep_dropoff_datetime IS NULL))
              AND (ISNULL(d2.passenger_count, -1) = ISNULL(d.passenger_count, -1))
              AND d2.id < d.id
        );
        
        DECLARE @DuplicatesFound INT = @@ROWCOUNT;
        DECLARE @RemainingRecords INT = (SELECT COUNT(*) FROM dbo.csv_data);
        
        -- Commit if everything succeeded
        COMMIT TRANSACTION;
        
        -- Return success structure
        INSERT INTO @Result VALUES (
            1,
            @DuplicatesFound,
            @RemainingRecords,
            NULL,
            NULL,
            NULL
        );
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
            
        -- Return error structure
        INSERT INTO @Result VALUES (
            0,
            0,
            0,
            ERROR_NUMBER(),
            ERROR_MESSAGE(),
            ERROR_LINE()
        );
    END CATCH
    
    SELECT * FROM @Result;
END
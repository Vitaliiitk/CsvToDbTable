CREATE DATABASE MyCsvDb;
GO

USE MyCsvDb;
GO

CREATE TABLE csv_data (
    id INT IDENTITY(1,1) PRIMARY KEY,
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
GO

-- Add computed column for travel time (Find the top 100 longest fares in terms of time spent traveling.)
ALTER TABLE csv_data
ADD travel_time AS DATEDIFF(SECOND, tpep_pickup_datetime, tpep_dropoff_datetime) PERSISTED;
GO

CREATE NONCLUSTERED INDEX idx_csv_data_pu_location_avg_tip 
ON csv_data(pu_location_id)
INCLUDE (tip_amount);
GO

CREATE NONCLUSTERED INDEX idx_csv_data_trip_distance 
ON csv_data(trip_distance DESC);
GO

CREATE NONCLUSTERED INDEX idx_csv_data_travel_time 
ON csv_data(travel_time DESC);
GO
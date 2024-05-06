USE ROLE CONTAINER_USER_ROLE;
USE DATABASE CONTAINER_HOL_DB;
USE SCHEMA PUBLIC;
USE WAREHOUSE CONTAINER_HOL_WH;

INSERT INTO weather (DATE, LOCATION, TEMP_C, TEMP_F)
    VALUES 
        ('2023-03-21', 'London', 15, NULL),
        ('2023-07-13', 'Manchester', 20, NULL),
        ('2023-05-09', 'Liverpool', 17, NULL),
        ('2023-09-17', 'Cambridge', 19, NULL),
        ('2023-11-02', 'Oxford', 13, NULL),
        ('2023-01-25', 'Birmingham', 11, NULL),
        ('2023-08-30', 'Newcastle', 21, NULL),
        ('2023-06-15', 'Bristol', 16, NULL),
        ('2023-04-07', 'Leeds', 18, NULL),
        ('2023-10-23', 'Southampton', 12, NULL);

CREATE OR REPLACE FUNCTION convert_udf (input float)
RETURNS float
SERVICE=CONVERT_API      //Snowflake container service
ENDPOINT='convert-api'   //The endpoint within the container
MAX_BATCH_ROWS=5         //limit the size of the batch
AS '/convert';           //The API endpoint

SELECT convert_udf(12) as conversion_result;
UPDATE WEATHER
SET TEMP_F = convert_udf(TEMP_C);

SELECT * FROM WEATHER;
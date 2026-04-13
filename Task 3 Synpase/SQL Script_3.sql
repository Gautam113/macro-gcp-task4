-- Export grid_readings to ADLS Gen2 as CSV

CREATE EXTERNAL DATA SOURCE adls_export
WITH (
    LOCATION = 'abfss://synapsefiles@energygridadls1.dfs.core.windows.net/'
);

-- Create external file format
CREATE EXTERNAL FILE FORMAT csv_format
WITH (
    FORMAT_TYPE = DELIMITEDTEXT,
    FORMAT_OPTIONS (
        FIELD_TERMINATOR = ',',
        STRING_DELIMITER = '"',
        FIRST_ROW = 2,
        USE_TYPE_DEFAULT = TRUE
    )
);

-- Export grid_readings to ADLS
CREATE EXTERNAL TABLE dbo.grid_readings_export1
WITH (
    LOCATION = 'snowflake-export/grid_readings/',
    DATA_SOURCE = adls_export,
    FILE_FORMAT = csv_format
)
AS
SELECT * FROM [dbo].[grid_readings_stream];
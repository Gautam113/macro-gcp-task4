-- grid_nodes dimension table
CREATE TABLE dbo.grid_nodes
(
    node_id              VARCHAR(20)   NOT NULL,
    node_name            VARCHAR(100),
    node_type            VARCHAR(50),
    region               VARCHAR(50),
    state                VARCHAR(5),
    capacity_mw          DECIMAL(10,2),
    is_renewable         BIT,
    commission_year      INTEGER,
    maintenance_schedule VARCHAR(50),
    lat                  DECIMAL(9,6),
    lon                  DECIMAL(9,6)
)
WITH
(
    DISTRIBUTION = REPLICATE,
    CLUSTERED COLUMNSTORE INDEX
);


COPY INTO dbo.grid_nodes
FROM 'https://energygridadls1.dfs.core.windows.net/synapsefiles/grid_nodes.csv'
WITH (
    FILE_TYPE = 'CSV',
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n'
);
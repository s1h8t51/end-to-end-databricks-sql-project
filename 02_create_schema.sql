-- Create schemas (databases) within the catalog
-- Schema = Logical grouping of tables

USE CATALOG production_data;

-- Bronze layer: Raw data ingestion
CREATE SCHEMA IF NOT EXISTS bronze
COMMENT 'Raw ingested data - no transformations';

-- Silver layer: Cleaned/validated data
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Cleaned and validated data';

-- Gold layer: Business-ready aggregations
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Business-ready aggregated data';

-- ML/AI layer: AI model outputs
CREATE SCHEMA IF NOT EXISTS ml_layer
COMMENT 'AI/ML classification results';

-- Grant permissions on schemas
GRANT USE SCHEMA ON SCHEMA bronze TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA silver TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA gold TO `data_engineers`;
GRANT USE SCHEMA ON SCHEMA ml_layer TO `data_engineers`;

-- Show schemas to verify
SHOW SCHEMAS;

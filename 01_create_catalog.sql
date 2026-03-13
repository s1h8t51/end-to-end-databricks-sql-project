-- Create Unity Catalog for the project
-- Unity Catalog = Data governance layer in Databricks

-- Create main catalog
CREATE CATALOG IF NOT EXISTS production_data
COMMENT 'Production data catalog for real-time analytics';

-- Grant permissions
GRANT USE CATALOG ON CATALOG production_data TO `data_engineers`;
GRANT CREATE SCHEMA ON CATALOG production_data TO `data_engineers`;

-- Show catalogs to verify
SHOW CATALOGS;

-- Set default catalog
USE CATALOG production_data;

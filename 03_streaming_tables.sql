-- Purpose: Create streaming tables for real-time data ingestion
-- Create streaming tables for real-time data ingestion
-- Streaming tables = Auto-updating tables that process data as it arrives

USE CATALOG production_data;
USE SCHEMA bronze;

-- Bronze: Raw streaming data from source
CREATE OR REFRESH STREAMING TABLE raw_events
COMMENT 'Raw event data from Kafka/Event Hub'
AS SELECT 
  current_timestamp() as ingestion_time,
  *
FROM cloud_files(
  '/mnt/landing/events/',
  'json',
  map(
    'cloudFiles.inferColumnTypes', 'true',
    'cloudFiles.schemaLocation', '/mnt/schemas/events'
  )
);

-- Silver: Cleaned streaming data
USE SCHEMA silver;

CREATE OR REFRESH STREAMING TABLE cleaned_events
COMMENT 'Cleaned and validated event data'
AS SELECT 
  event_id,
  user_id,
  event_type,
  event_timestamp,
  CAST(event_value AS DOUBLE) as event_value,
  event_metadata
FROM STREAM(production_data.bronze.raw_events)
WHERE event_id IS NOT NULL
  AND event_timestamp IS NOT NULL;

-- Gold: Aggregated streaming data
USE SCHEMA gold;

CREATE OR REFRESH STREAMING TABLE event_summary
COMMENT 'Aggregated event metrics by user and type'
AS SELECT 
  user_id,
  event_type,
  window(event_timestamp, '1 hour') as time_window,
  COUNT(*) as event_count,
  AVG(event_value) as avg_value,
  MAX(event_value) as max_value
FROM STREAM(production_data.silver.cleaned_events)
GROUP BY user_id, event_type, window(event_timestamp, '1 hour');

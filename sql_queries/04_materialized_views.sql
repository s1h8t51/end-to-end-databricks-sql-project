-- Create materialized views for optimized query performance
-- Materialized View = Pre-computed query results that update automatically

USE CATALOG production_data;
USE SCHEMA gold;

-- Daily user activity summary
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_user_activity
COMMENT 'Pre-aggregated daily user metrics'
AS SELECT 
  DATE(event_timestamp) as activity_date,
  user_id,
  COUNT(DISTINCT event_id) as total_events,
  COUNT(DISTINCT event_type) as unique_event_types,
  SUM(event_value) as total_value,
  MIN(event_timestamp) as first_event,
  MAX(event_timestamp) as last_event
FROM production_data.silver.cleaned_events
GROUP BY DATE(event_timestamp), user_id;

-- Event type performance metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS event_type_metrics
COMMENT 'Performance metrics by event type'
AS SELECT 
  event_type,
  DATE(event_timestamp) as metric_date,
  COUNT(*) as event_count,
  AVG(event_value) as avg_value,
  STDDEV(event_value) as stddev_value,
  PERCENTILE_APPROX(event_value, 0.5) as median_value,
  PERCENTILE_APPROX(event_value, 0.95) as p95_value
FROM production_data.silver.cleaned_events
GROUP BY event_type, DATE(event_timestamp);

-- High-value users
CREATE MATERIALIZED VIEW IF NOT EXISTS high_value_users
COMMENT 'Users with high engagement or value'
AS SELECT 
  user_id,
  SUM(event_value) as lifetime_value,
  COUNT(DISTINCT event_id) as total_events,
  COUNT(DISTINCT DATE(event_timestamp)) as active_days,
  MAX(event_timestamp) as last_seen
FROM production_data.silver.cleaned_events
GROUP BY user_id
HAVING SUM(event_value) > 1000 OR COUNT(DISTINCT event_id) > 100;

-- Refresh materialized views (optional - they auto-refresh)
-- REFRESH MATERIALIZED VIEW daily_user_activity;

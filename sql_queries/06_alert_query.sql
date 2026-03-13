
-- Create alerts for anomalies and critical business conditions
-- Databricks Alerts = Automated notifications based on query results

USE CATALOG production_data;

-- Alert 1: High-priority events requiring immediate attention
CREATE OR REPLACE ALERT high_priority_events
  QUERY 'SELECT 
    COUNT(*) as high_priority_count,
    COLLECT_LIST(event_id) as event_ids
  FROM production_data.ml_layer.realtime_classification
  WHERE priority_level = ''high_priority''
    AND processed_at > current_timestamp() - INTERVAL 1 HOUR'
  CONDITION 'high_priority_count > 10'
  ACTIONS email('data-team@company.com', 'High Priority Events Detected');

-- Alert 2: Anomaly detection - flag unusual patterns
CREATE OR REPLACE ALERT anomaly_detected
  QUERY 'SELECT 
    user_id,
    COUNT(*) as anomaly_count,
    AVG(anomaly_score) as avg_anomaly_score
  FROM production_data.ml_layer.anomaly_detection
  WHERE anomaly_score > 0.9
    AND event_timestamp > current_timestamp() - INTERVAL 15 MINUTES
  GROUP BY user_id'
  CONDITION 'anomaly_count > 5'
  ACTIONS 
    email('security-team@company.com', 'Potential Fraud Detected'),
    webhook('https://slack.com/webhook/security-alerts');

-- Alert 3: Negative sentiment spike
CREATE OR REPLACE ALERT negative_sentiment_spike
  QUERY 'SELECT 
    COUNT(*) as negative_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) 
      FROM production_data.ml_layer.event_classification 
      WHERE classification_timestamp > current_timestamp() - INTERVAL 1 HOUR
    ) as negative_percentage
  FROM production_data.ml_layer.event_classification
  WHERE sentiment = ''negative''
    AND classification_timestamp > current_timestamp() - INTERVAL 1 HOUR'
  CONDITION 'negative_percentage > 30'  -- More than 30% negative
  ACTIONS email('product-team@company.com', 'Negative Sentiment Spike');

-- Alert 4: Data quality issues
CREATE OR REPLACE ALERT data_quality_check
  QUERY 'SELECT 
    COUNT(*) as null_count,
    COUNT(*) * 100.0 / (SELECT COUNT(*) 
      FROM production_data.bronze.raw_events 
      WHERE ingestion_time > current_timestamp() - INTERVAL 1 HOUR
    ) as null_percentage
  FROM production_data.bronze.raw_events
  WHERE (event_id IS NULL OR user_id IS NULL)
    AND ingestion_time > current_timestamp() - INTERVAL 1 HOUR'
  CONDITION 'null_percentage > 5'  -- More than 5% nulls
  ACTIONS email('data-engineers@company.com', 'Data Quality Issue Detected');

-- Alert 5: Pipeline health check
CREATE OR REPLACE ALERT pipeline_lag_check
  QUERY 'SELECT 
    MAX(event_timestamp) as latest_event,
    current_timestamp() - MAX(event_timestamp) as lag_seconds
  FROM production_data.silver.cleaned_events'
  CONDITION 'lag_seconds > 300'  -- More than 5 minutes lag
  ACTIONS email('devops@company.com', 'Pipeline Lag Detected');

-- Alert 6: High-value transaction monitoring
CREATE OR REPLACE ALERT high_value_transactions
  QUERY 'SELECT 
    user_id,
    event_id,
    event_value,
    event_timestamp
  FROM production_data.silver.cleaned_events
  WHERE event_type = ''transaction''
    AND event_value > 10000
    AND event_timestamp > current_timestamp() - INTERVAL 5 MINUTES'
  CONDITION 'COUNT(*) > 0'
  ACTIONS 
    email('finance-team@company.com', 'High-Value Transaction Alert'),
    webhook('https://company.com/api/fraud-check');

-- View all alerts

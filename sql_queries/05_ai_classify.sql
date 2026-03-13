
-- AI-powered classification using Databricks AI Functions
-- Uses built-in LLMs (e.g., DBRX, Llama) for text classification

USE CATALOG production_data;
USE SCHEMA ml_layer;

-- Create table with AI classification results
CREATE OR REPLACE TABLE event_classification
COMMENT 'AI-classified event data with sentiment and category'
AS SELECT 
  event_id,
  user_id,
  event_type,
  event_timestamp,
  event_metadata,
  
  -- Sentiment analysis using AI
  ai_classify(
    event_metadata,
    ARRAY('positive', 'negative', 'neutral')
  ) as sentiment,
  
  -- Category classification
  ai_classify(
    event_metadata,
    ARRAY('transaction', 'login', 'error', 'notification', 'other')
  ) as event_category,
  
  -- Extract key entities (if text data)
  ai_extract(
    event_metadata,
    ARRAY('person', 'organization', 'location', 'product')
  ) as extracted_entities,
  
  -- Generate summary (if long text)
  ai_summarize(event_metadata, 50) as event_summary,
  
  current_timestamp() as classification_timestamp
  
FROM production_data.silver.cleaned_events
WHERE event_metadata IS NOT NULL;

-- Create streaming version for real-time classification
CREATE OR REFRESH STREAMING TABLE realtime_classification
COMMENT 'Real-time AI classification of incoming events'
AS SELECT 
  event_id,
  user_id,
  event_type,
  ai_classify(
    event_metadata,
    ARRAY('positive', 'negative', 'neutral')
  ) as sentiment,
  ai_classify(
    event_metadata,
    ARRAY('high_priority', 'medium_priority', 'low_priority')
  ) as priority_level,
  current_timestamp() as processed_at
FROM STREAM(production_data.silver.cleaned_events)
WHERE event_metadata IS NOT NULL;

-- Anomaly detection using AI (if supported)
CREATE OR REPLACE TABLE anomaly_detection
AS SELECT 
  event_id,
  user_id,
  event_value,
  ai_anomaly_score(event_value) OVER (
    PARTITION BY user_id, event_type
    ORDER BY event_timestamp
    ROWS BETWEEN 100 PRECEDING AND CURRENT ROW
  ) as anomaly_score
FROM production_data.silver.cleaned_events
WHERE anomaly_score > 0.8;  -- Flag high anomaly scores

-- AI Functions Available:
-- ai_classify() - Classify text into categories
-- ai_extract() - Extract named entities
-- ai_summarize() - Generate summaries
-- ai_sentiment() - Sentiment analysis
-- ai_translate() - Language translation

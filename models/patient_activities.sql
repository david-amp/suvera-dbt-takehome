-- Create a new table for patient_activities
{{ config(materialized='table') }}

-- Remove any duplicates from the raw_pcns seed
WITH patient_activities_distinct AS(
    SELECT DISTINCT * FROM {{ ref('raw_activities') }}
),

-- Clean duration_minutes column
patient_activities AS (
    SELECT 
    activity_id::INTEGER AS activity_id,
    patient_id::INTEGER AS patient_id,
    activity_type::STRING AS activity_type,
    activity_date::DATETIME as activity_date_time,
    CASE
        WHEN duration_minutes::STRING LIKE '%-%' THEN NULL
        ELSE duration_minutes::INTEGER
        END AS duration_minutes
FROM patient_activities_distinct
)

SELECT * FROM patient_activities

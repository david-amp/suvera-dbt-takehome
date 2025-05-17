{{ config(materialized='table') }}

-- Apply a window function to be sure we can also return the activity_type for more clarity
WITH ranked_activities AS (
    SELECT
    patient_id,
    activity_type,
    activity_date_time::DATE AS activity_date,
    ROW_NUMBER() OVER (
        PARTITION BY patient_id
        ORDER BY activity_date
    ) AS ranking
    FROM {{ ref('patient_activities') }}
)

-- Return most recent activity by ranking
SELECT 
patient_id,
activity_type,
activity_date AS latest_activty_date
FROM ranked_activities
where ranking = 1

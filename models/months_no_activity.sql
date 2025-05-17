{{ config(materialized='table') }}

-- Find patient's with at least 1 activitye
WITH first_activities AS (
  SELECT
    patient_id,
    MIN(activity_date_time::DATE) AS first_activity_date
  FROM {{ ref('patient_activities') }}
  GROUP BY patient_id
),

-- Find those WITH an activity within 3 months, then we can anti join to answer the question
activity_within_3_months AS (
  SELECT
    pa.patient_id
  FROM patient_activities pa
  JOIN first_activities fa ON pa.patient_id = fa.patient_id
  WHERE 
    pa.activity_date_time::DATE > fa.first_activity_date
    AND pa.activity_date_time::DATE <= DATEADD(month, 3, fa.first_activity_date)
),

-- Anti join to find all patients without activity within 3 months of first activity
no_activity_after_3_months AS (
  SELECT
    fa.patient_id,
    fa.first_activity_date
  FROM first_activities fa
  LEFT JOIN activity_within_3_months aw3m ON fa.patient_id = aw3m.patient_id
  WHERE aw3m.patient_id IS NULL
)

SELECT * FROM no_activity_after_3_months

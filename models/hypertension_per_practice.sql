{{ config(materialized='table') }}

-- Find all hypertension patients
WITH hypertension_patients_raw AS (
  SELECT
  pc.patient_id,
  pd.practice_id
  FROM {{ ref('patient_conditions') }} pc
  JOIN {{ ref('patient_data') }} pd ON pc.patient_id = pd.patient_id
  WHERE pc.condition = 'hypertension'
),

-- Patients grouped by practice (known or unknown)
hypertension_patients AS (
    SELECT
    CASE 
        WHEN p.id IS NULL THEN NULL
        ELSE htpr.practice_id
        END AS practice_id,
    COUNT(DISTINCT htpr.patient_id) AS hypertension_patient_count
    FROM hypertension_patients_raw htpr
    LEFT JOIN {{ ref('practices') }} p ON htpr.practice_id = p.id
    GROUP BY CASE WHEN p.id IS NULL THEN NULL ELSE htpr.practice_id END
),

-- All patients grouped by practice (known or unknown)
patients_per_practice AS (
    SELECT
    CASE 
        WHEN p.id IS NULL THEN NULL
        ELSE pd.practice_id
        END AS practice_id,
    COUNT(DISTINCT pd.patient_id) AS total_patients
    FROM {{ ref('patient_data') }} pd
    LEFT JOIN {{ ref('practices') }} p ON pd.practice_id = p.id
    GROUP BY CASE WHEN p.id IS NULL THEN NULL ELSE pd.practice_id END
)

-- Handle unknown practices and calculate patient hypertension percentage
SELECT
CASE 
    WHEN p.practice_name IS NULL THEN 'Uknown'
    ELSE p.practice_name
    END AS practice_name,
CASE 
    WHEN htp.hypertension_patient_count IS NULL THEN 0
    ELSE htp.hypertension_patient_count
    END AS hypertension_patient_count,
CASE 
    WHEN ppp.total_patients = 0 THEN 0
    ELSE ppp.total_patients
    END AS total_patients,
CASE 
    WHEN htp.hypertension_patient_count IS NULL THEN 0
    ELSE ROUND(100.0 * htp.hypertension_patient_count / ppp.total_patients, 2) 
    END AS hypertension_pct
FROM patients_per_practice ppp
LEFT JOIN hypertension_patients htp ON ppp.practice_id = htp.practice_id
LEFT JOIN {{ ref('practices') }} p ON htp.practice_id = p.id

{{ config(materialized='table') }}

-- Count patients per pcn
WITH patients_per_pcn AS (
    SELECT  
    COUNT(pd.patient_id) AS patients, 
    pcns.pcn_name AS pcn
    FROM {{ ref('patient_data') }} pd
    LEFT JOIN {{ ref('practices') }} p ON pd.practice_id = p.id
    LEFT JOIN {{ ref('pcns') }} ON p.pcn = pcns.id
    GROUP BY pcns.pcn_name
)

-- Handle patients not associated with a pcn
SELECT 
patients,
CASE
    WHEN pcn IS NULL THEN 'unknown'
    ELSE pcn
    END AS pcn
FROM patients_per_pcn

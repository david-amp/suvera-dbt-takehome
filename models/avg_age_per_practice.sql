{{ config(materialized='table') }}

-- Set a limit on age <= 100, as ages older than 100 are likely anomalous and should be discounted
SELECT 
ROUND(AVG(pd.age)) AS average_age, 
p.practice_name 
FROM {{ ref('patient_data') }} pd
JOIN practices p ON pd.practice_id = p.id
WHERE NOT pd.check_age
GROUP BY p.practice_name

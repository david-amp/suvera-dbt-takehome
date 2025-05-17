 {{ config(materialized='table') }}

-- Filtering out ages >100 as likely anomolous values
WITH patient_age_groups AS (
    SELECT 
    patient_id,
    practice_id,
    CASE 
        WHEN age BETWEEN 0 AND 18 THEN '0-18'
        WHEN age BETWEEN 19 AND 35 THEN '19-35'
        WHEN age BETWEEN 36 AND 50 THEN '36-50'
        WHEN age >= 51 THEN '51+'
        ELSE 'Unknon'
        END AS age_group
    FROM {{ ref('patient_data') }} 
    WHERE NOT check_age
),

-- Count the number of patients per age group per practice
age_group_per_pcn AS (
    SELECT
    COUNT(pag.patient_id) AS patients,
    pag.age_group,
    pcns.pcn_name
    FROM patient_age_groups pag
    JOIN {{ ref('practices') }} p ON pag.practice_id = p.id
    JOIN {{ ref('pcns') }} ON p.pcn = pcns.id
    GROUP BY pcns.pcn_name, pag.age_group
    ORDER BY pcns.pcn_name, pag.age_group
)

SELECT * FROM age_group_per_pcn

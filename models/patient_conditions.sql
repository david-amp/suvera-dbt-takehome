-- Create a new one-to-many table for patient conditions
{{ config(materialized='table') }}

-- Remove any duplicates from the raw_patients seed and parse out the JSON schemas
WITH distinct_patient_raw AS(
    SELECT DISTINCT * FROM {{ ref('raw_patients') }}
),
-- Parse the JSON schemas out of the raw_patients.csv file
parsed_raw_patients AS (
  SELECT 
    PARSE_JSON(data) AS json_data
  FROM distinct_patient_raw
),

-- Instantiate the STRING datatype on conditions to remove unecessary quotes, minimises data cleaning steps
-- Flatten JSON array to extract each condition per patient. 
patient_conditions AS (
    SELECT
    json_data:patient_id::INTEGER AS patient_id,
    condition.value::STRING AS condition
    FROM parsed_raw_patients,
    LATERAL FLATTEN(INPUT => json_data:conditions) AS condition
)

SELECT * FROM patient_conditions

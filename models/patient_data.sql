-- Create a new one-to-many table for patient data
{{ config(materialized='table') }}

-- Remove any duplicates from the raw_patients seed and parse out the JSON schemas
WITH distinct_patient_raw AS (
    SELECT DISTINCT * FROM {{ ref('raw_patients') }}
),

-- Parse the JSON schemas out of the raw_patients.csv file
patient_data_parsed AS (
  SELECT 
    PARSE_JSON(data) AS json_data
  FROM distinct_patient_raw
),

-- Cleaning the parsed patient data
patient_data_parsed_format AS (
    SELECT 
    json_data:patient_id::STRING::INTEGER AS patient_id,
    case 
        when json_data:practice_id::STRING = 'null' THEN NULL 
        when json_data:practice_id::STRING = 'invalid' THEN NULL 
        else json_data:practice_id::STRING::INTEGER
        END AS practice_id,
    case 
        when json_data:age::STRING = 'unknown' THEN NULL 
        when json_data:age::STRING LIKE '-%' THEN NULL
        when json_data:age::STRING = 'null' THEN NULL
        else json_data:age::STRING::INTEGER 
        END AS age,
    -- Set a flag for ages > 100 as these are likely anomolous and to be checked. Can filter on this for age related queries.
    CASE
        WHEN age > 100 THEN TRUE
        ELSE FALSE
        END AS check_age,
    -- Assuming F and M are the only genders being collected here
    CASE 
        WHEN json_data:gender::STRING NOT IN ('F', 'M') THEN NULL 
        ELSE json_data:gender::STRING 
        END AS gender,
    CASE 
        WHEN json_data:registration_date::STRING = 'null' THEN NULL
        ELSE json_data:registration_date::STRING::DATE 
        END AS registration_date,
    CASE    
        WHEN json_data:contact:email::STRING NOT LIKE '%@%' THEN NULL 
        ELSE json_data:contact:email::STRING 
        END AS email,
    REGEXP_REPLACE(TRIM(json_data:contact:phone::STRING), '[^0-9]') AS phone_numerical,
    -- Assuming a valid phone number has 10 or 11 digits
    LENGTH(phone_numerical) BETWEEN 10 AND 11 AS is_valid_phone
FROM patient_data_parsed 
),

-- Only show "valid" phone numbers
patinet_data AS (
    SELECT
    *,
    CASE
        WHEN is_valid_phone THEN phone_numerical 
        ELSE NULL
        END AS phone
FROM patient_data_parsed_format
)

-- Exclude the is_phone_valid and phone_numerical columns. Ideally this would be done with a macro.
SELECT 
    patient_id,
    practice_id,
    age,
    check_age,
    gender,
    registration_date,
    email,
    phone
FROM patinet_data

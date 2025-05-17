-- Create a new table for practices
{{ config(materialized='table') }}

-- Remove any duplicates from the raw_practices seed
WITH practices_distinct AS(
    SELECT DISTINCT * FROM {{ ref('raw_practices') }}
)

select * from practices_distinct

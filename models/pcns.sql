-- Create a new table for pcns
{{ config(materialized='table') }}

-- Remove any duplicates from the raw_pcns seed
WITH pcns AS(
    SELECT DISTINCT * FROM {{ ref('raw_pcns') }}
)

SELECT * FROM pcns
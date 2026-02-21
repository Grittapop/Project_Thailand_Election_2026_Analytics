{{ config(
    materialized='ephemeral',
    )
}}

WITH ods_party AS (

    SELECT
        party_id,
        party_no,
        party_name,
        party_abbr
    FROM 
        {{ source('silver', 'ods_party') }}

)

SELECT
    -1 as party_id,
    null as party_no,
    'Unknown' as party_name,
    null as party_abbr

UNION ALL

SELECT 
    * 
FROM 
    ods_party

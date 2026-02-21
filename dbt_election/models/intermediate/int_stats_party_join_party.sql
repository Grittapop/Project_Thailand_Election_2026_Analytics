{{ config(
    materialized='ephemeral',
    )
}}

WITH ods_stats_party AS (

    SELECT
        *
    FROM 
        {{ source('silver', 'ods_stats_party') }}

)

    SELECT
        o.*,
        d.party_id as dim_party_id
    FROM 
        ods_stats_party o
    LEFT JOIN 
        {{ ref('dim_party') }} d
    ON 
        o.party_id = d.party_id



{{ config(
    materialized='ephemeral',
    )
}}

WITH ods_mp_candidate AS (

    SELECT
        mp_candidate_id,
        candidate_no,
        party_id,
        candidate_name
    FROM {{ source('silver', 'ods_mp_candidate') }}

)

    SELECT
        o.mp_candidate_id,
        o.candidate_no,
        d.party_id AS dim_party_id,
        o.candidate_name
    FROM 
        ods_mp_candidate o
    LEFT JOIN 
        {{ ref('dim_party') }} d
    ON 
        o.party_id = d.party_id


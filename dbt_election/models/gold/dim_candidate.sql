WITH base AS (

    SELECT
        mp_candidate_id,
        candidate_no,
        party_id,
        candidate_name
    FROM {{ source('silver', 'ods_mp_candidate') }}

),

joined AS (

    SELECT
        b.mp_candidate_id,
        b.candidate_no,
        d.party_id AS dim_party_id,
        b.candidate_name
    FROM base b
    LEFT JOIN {{ ref('dim_party') }} d
        ON b.party_id = d.party_id

)

SELECT
    mp_candidate_id,
    candidate_no,
    COALESCE(dim_party_id, -1) AS party_id,
    candidate_name
FROM joined
    
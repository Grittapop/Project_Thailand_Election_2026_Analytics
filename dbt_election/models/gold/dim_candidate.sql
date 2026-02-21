SELECT
    mp_candidate_id,
    candidate_no,
    COALESCE(dim_party_id, -1) AS party_id,
    candidate_name
FROM 
    {{ ref('int_candidate_join_party') }}
    
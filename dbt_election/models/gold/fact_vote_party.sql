WITH base AS (

    SELECT
        *
    FROM 
        {{ source('silver', 'ods_stats_party') }}

),

joined AS (

    SELECT
        b.*,
        d.party_id as dim_party_id
    FROM 
        base b
    LEFT JOIN 
        {{ ref('dim_party') }} d
    ON 
        b.party_id = d.party_id

)

SELECT
    coalesce(dim_party_id, -1) as party_id,
    party_vote,
    party_vote_percent,
    mp_app_vote,
    mp_app_vote_percent,
    first_mp_app_count,
    counted_vote_stations,
    percent_count

FROM 
    joined
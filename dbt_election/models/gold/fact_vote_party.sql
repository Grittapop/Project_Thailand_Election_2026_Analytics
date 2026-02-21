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
    {{ ref('int_stats_party_join_party') }}
SELECT 
    constituency_id ,
    province_id ,
    coalesce(party_id, -1) as party_id,
    vote ,
    vote_percent ,
    rank ,
    turn_out ,
    percent_turn_out ,
    valid_votes ,
    invalid_votes ,
    blank_votes 
   
FROM
    {{ source('silver', 'ods_stats_cons') }}
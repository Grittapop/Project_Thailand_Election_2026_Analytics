SELECT 
    constituency_id ,
    constituency_no ,
    prov_id ,
    zone ,
    total_vote_stations ,
    registered_vote 
FROM
    {{ source('silver', 'ods_constituency') }}
    
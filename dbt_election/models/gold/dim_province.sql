SELECT 
    province_id ,
    prov_id ,
    province ,
    abbre_thai,
    eng     
FROM
    {{ source('silver', 'ods_province') }}
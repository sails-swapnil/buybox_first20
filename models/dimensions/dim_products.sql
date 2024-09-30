SELECT
    *,  
   UUID_STRING() AS swa,
FROM 
    {{ ref('src_products') }}
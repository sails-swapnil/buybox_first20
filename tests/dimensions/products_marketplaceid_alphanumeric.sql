WITH invalid_marketplace_ids AS (
    SELECT
        MarketplaceId
    FROM BUYBOX.RAW.dim_products
    WHERE MarketplaceId NOT RLIKE '^[a-zA-Z0-9]+$'
)

SELECT
    CASE 
        WHEN COUNT(*) = 0 THEN NULL  
        ELSE COUNT(*)            
    END AS invalid_count
FROM invalid_marketplace_ids 
HAVING COUNT(*) >= 1

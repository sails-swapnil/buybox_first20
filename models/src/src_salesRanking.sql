WITH source_data AS(
    SELECT PARSE_JSON(message_body) AS raw_data
    FROM {{ source( 'buybox', 'buybox_raw' )}}
),
flatten_payload AS
(
    SELECT 
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"SalesRankings"::ARRAY AS offer,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::OBJECT:"ASIN"::STRING AS ASIN
    FROM    
        source_data
)

SELECT 
    ASIN,
    offer.value:"ProductCategoryId" :: STRING AS ProductCategoryId,
    offer.value:"Rank" :: STRING AS Rank
FROM flatten_payload f,
LATERAL FLATTEN(input => f.offer) AS offer

WITH source_data AS(
    SELECT PARSE_JSON(message_body) AS raw_data
    FROM {{ source( 'buybox', 'buybox_raw' )}}
),
flatten_payload AS
(
    SELECT 
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY AS offer
    FROM    
        source_data
)

SELECT 
    offer.value:"ASIN"::STRING AS ASIN,
    offer.value:"MarketplaceId" :: STRING AS MarketplaceId
FROM flatten_payload f,
LATERAL FLATTEN(input => f.offer) AS offer

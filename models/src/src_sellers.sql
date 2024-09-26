{{
    config(
        materialized = 'incremental',
        unique_key = 'SellerId',
        merge_update_columns=['IsFeaturedMerchant', 'IsFulfilledByAmazon']
    )
}}

WITH source_data AS(
    SELECT PARSE_JSON(message_body) AS raw_data
    FROM {{ source( 'buybox', 'buybox_raw' )}}
),
flatten_payload AS
(
    SELECT 
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY AS offers
    FROM    
        source_data
)

SELECT 
    offer.value:"SellerId"::STRING AS SellerId,
    offer.value:"IsFeaturedMerchant":: Boolean AS IsFeaturedMerchant,
    offer.value:"IsFulfilledByAmazon":: Boolean AS IsFulfilledByAmazon
FROM flatten_payload f,
LATERAL FLATTEN(input => f.offers) AS offer

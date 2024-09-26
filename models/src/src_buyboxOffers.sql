WITH source_data AS (
    SELECT PARSE_JSON(message_body) AS raw_data
    FROM {{ source('buybox', 'buybox_raw') }}
),
flatten_payload AS (
    SELECT 
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfOffers"::ARRAY AS numbers,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"NumberOfBuyBoxEligibleOffers"::ARRAY AS eligibles
    FROM source_data
),
flatten_offers AS (
    SELECT 
        eligible_item.value:"Condition"::STRING AS EligibleOfferCondition,
        eligible_item.value:"FulfillmentChannel"::STRING AS EligibleOfferFullfillmentChannel,        
        eligible_item.value:"OfferCount"::FLOAT AS EligibleOfferCount,
        number_item.value:"Condition"::STRING AS NumberOfferCondition,
        number_item.value:"FulfillmentChannel"::STRING AS NumberOfferFullfillmentChannel,
        number_item.value:"OfferCount"::STRING AS NumberOfferCount

    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.eligibles) AS eligible_item,
    LATERAL FLATTEN(input => flatten_payload.numbers) AS number_item

)

SELECT 
    EligibleOfferCondition,
    EligibleOfferFullfillmentChannel,
    EligibleOfferCount,
    NumberOfferCondition,
    NumberOfferFullfillmentChannel,
    NumberOfferCount
FROM 
    flatten_offers
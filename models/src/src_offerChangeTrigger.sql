WITH source_data AS (
    SELECT PARSE_JSON(message_body) AS raw_data,
           raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY AS offer_array
    FROM {{ source('buybox', 'buybox_raw') }}
),
flatten_payload AS (
    SELECT 
        offer_item.value:"ItemCondition"::STRING AS ItemCondition,
        offer_item.value:"ASIN"::STRING AS ASIN,
        raw_data:"NotificationMetadata"::OBJECT:"PublishTime"::TIMESTAMP AS PublishTime
    FROM source_data,
    LATERAL FLATTEN(input => source_data.offer_array) AS offer_item
)


SELECT 
    ASIN,
    ItemCondition,
    PublishTime
FROM flatten_payload



WITH source_data AS (
    SELECT PARSE_JSON(message_body) AS raw_data
    FROM {{ source('buybox', 'buybox_raw') }}
),
flatten_payload AS (
    SELECT 
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"LowestPrices"::ARRAY AS summaries
    FROM source_data
),
flatten_offers AS (
    SELECT 
        NotificationId,
        offer_item.value:"Condition"::STRING AS Condition,
        offer_item.value:"FulfillmentChannel"::STRING AS FulfillmentChannel,        
        offer_item.value:"LandedPrice"::OBJECT:"Amount"::FLOAT AS LandedPriceAmount,
        offer_item.value:"LandedPrice"::OBJECT:"CurrencyCode"::STRING AS LandedPriceCurrencyCode,
        offer_item.value:"ListingPrice"::OBJECT:"Amount"::Float AS ListingPriceAmount,
        offer_item.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING AS ListingPriceCurrencyCode,
        offer_item.value:"Shipping"::OBJECT:"Amount"::Float AS ShippingAmount,
        offer_item.value:"Shipping"::OBJECT:"CurrencyCode"::STRING AS ShippingCurrencyCode,

    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.summaries) AS offer_item
)

SELECT 
    NotificationId,
    Condition,
    FulfillmentChannel,
    LandedPriceAmount,
    LandedPriceCurrencyCode,
    ListingPriceAmount,
    ListingPriceCurrencyCode,
    ShippingAmount,
    ShippingCurrencyCode
FROM 
    flatten_offers
WITH source_data AS (
    SELECT PARSE_JSON(message_body) AS raw_data,
           raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::ARRAY AS summary
    FROM {{ source('buybox', 'buybox_raw') }}
),
flatten_payload AS (
    SELECT 
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        summary_item.value:"ListPrice"::OBJECT:"Amount"::FLOAT AS ListPriceAmount,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Summary"::OBJECT:"BuyBoxPrices"::ARRAY AS summaries
    FROM source_data,
    LATERAL FLATTEN(input => source_data.summary) AS summary_item
),
flatten_offers AS (
    SELECT 
        NotificationId,
        ListPriceAmount,
        offer_item.value:"Shipping"::OBJECT:"Amount"::FLOAT AS ShippingAmount,
        offer_item.value:"LandedPrice"::OBJECT:"Amount"::FLOAT AS BB_LandingPrices_Amount,
        offer_item.value:"LandedPrice"::OBJECT:"CurrencyCode"::STRING AS BB_LandingPrices_CurrencyCode,
        offer_item.value:"ListingPrice"::OBJECT:"Amount"::Float AS BB_ListingPrice_Amount,
        offer_item.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING AS BB_ListingPrice_CurrencyCode,
        offer_item.value:"Shipping"::OBJECT:"Amount"::Float AS BB_ShippingCost,
        offer_item.value:"Shipping"::OBJECT:"CurrencyCode"::STRING AS BB_ShippingCurrencyCode,

    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.summaries) AS offer_item
)

SELECT 
    NotificationId,
    BB_LandingPrices_Amount,
    BB_LandingPrices_CurrencyCode,
    BB_ListingPrice_Amount,
    BB_ListingPrice_CurrencyCode,
    BB_ShippingCost,
    BB_ShippingCurrencyCode,
    ListPriceAmount
FROM 
    flatten_offers
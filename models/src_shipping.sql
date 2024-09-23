WITH source_data AS (
    SELECT PARSE_JSON(message_body) AS raw_data,
           raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY AS offer_array
    FROM {{ source('buybox', 'buybox_raw') }}
),
flatten_payload AS (
    SELECT 
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY AS offers_array
    FROM source_data,
    LATERAL FLATTEN(input => source_data.offer_array) AS offer_item
),
flatten_offers AS (
    SELECT 
        NotificationId,
        offer_item.value:"Shipping"::OBJECT:"Amount"::FLOAT AS ShippingAmount,
        offer_item.value:"Shipping"::OBJECT:"CurrencyCode"::STRING AS ShippingCurrencyCode,
        offer_item.value:"ShippingTime"::OBJECT:"AvailabilityType"::STRING AS ShippingAvailabilityType,
        NULLIF(offer_item.value:"ShippingTime"::OBJECT:"AvailableDate",'')::FLOAT AS ShippingAvailableDate,
        offer_item.value:"ShippingTime"::OBJECT:"MaximumHours"::FLOAT AS ShippingMaximumHours,
        offer_item.value:"ShippingTime"::OBJECT:"MinimumHours"::FLOAT AS ShippingMinimumHours,
        offer_item.value:"ShipsDomestically"::STRING AS ShipsDomestically
    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.offers_array) AS offer_item
)

SELECT 
    NotificationId,
    ShippingAmount,
    ShippingCurrencyCode,
    ShippingAvailabilityType,
    ShippingAvailableDate,
    ShippingMaximumHours,
    ShippingMinimumHours,
    ShipsDomestically
FROM 
    flatten_offers

WITH source_data AS (
    SELECT PARSE_JSON(message_body) AS raw_data,
           raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY AS offer_array
    FROM {{ source('buybox', 'buybox_raw') }}
),
flatten_payload AS (
    SELECT 
        raw_data:"EventTime"::TIMESTAMP AS EventTime,
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        offer_item.value:"ASIN"::STRING AS ASIN,
        raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"Offers"::ARRAY AS offers_array
    FROM source_data,
    LATERAL FLATTEN(input => source_data.offer_array) AS offer_item
),
flatten_offers AS (
    SELECT 
        EventTime,
        NotificationId,
        ASIN,
        ROW_NUMBER() OVER(PARTITION BY ASIN ORDER BY NotificationId DESC) AS rn, 
        offer_item.value:"SellerId"::STRING AS SellerId,
        offer_item.value:"IsBuyBoxWinner"::BOOLEAN AS IsBuyBoxWinner,
        offer_item.value:"ListingPrice"::OBJECT:"Amount"::FLOAT AS ListingPriceAmount,
        offer_item.value:"ListingPrice"::OBJECT:"CurrencyCode"::STRING AS ListingPriceCurrencyCode,
        offer_item.value:"PrimeInformation"::OBJECT:"IsOfferNationalPrime"::BOOLEAN AS PrimeInformation_IsOfferNationalPrime,
        offer_item.value:"PrimeInformation"::OBJECT:"IsOfferPrime"::BOOLEAN AS PrimeInformation_IsOfferPrime,
        offer_item.value:"SubCondition"::STRING AS Subcondition
    FROM flatten_payload,
    LATERAL FLATTEN(input => flatten_payload.offers_array) AS offer_item
)

SELECT 
    EventTime,
    rn AS OfferId,
    ASIN,
    NotificationId,
    SellerId,
    IsBuyBoxWinner,
    ListingPriceAmount,
    ListingPriceCurrencyCode,
    PrimeInformation_IsOfferNationalPrime,
    PrimeInformation_IsOfferPrime,
    Subcondition
FROM 
    flatten_offers
GROUP BY 
    OfferId,
    ASIN,
    NotificationId,
    SellerId,
    ListingPriceAmount,
    ListingPriceCurrencyCode,
    PrimeInformation_IsOfferNationalPrime,
    PrimeInformation_IsOfferPrime,
    Subcondition,
    IsBuyBoxWinner,
    EventTime
ORDER BY 
    OfferId

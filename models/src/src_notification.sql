WITH source_data AS(
    SELECT PARSE_JSON(message_body) AS raw_data,
    raw_data:"Payload"::OBJECT:"AnyOfferChangedNotification"::OBJECT:"OfferChangeTrigger"::ARRAY AS offer
    FROM {{ source( 'buybox', 'buybox_raw' )}}
),
flatten_data AS
(
    SELECT 
        raw_data:"NotificationMetadata"::OBJECT:"ApplicationId"::STRING AS ApplicationId,
        raw_data:"NotificationMetadata"::OBJECT:"NotificationId"::STRING AS NotificationId,
        raw_data:"NotificationMetadata"::OBJECT:"PublishTime"::TIMESTAMP AS PublishTime,
        raw_data:"NotificationMetadata"::OBJECT:"SubscriptionId"::STRING AS SubscriptionId,
        raw_data:"NotificationType"::STRING AS NotificationType,
        raw_data:"NotificationVersion"::STRING AS NotificationVersion,
        offer.value:"ASIN"::STRING AS ASIN,
    FROM    
        source_data,
    LATERAL FLATTEN(input=>source_data.offer) AS offer
)

SELECT 
    NotificationId,
    PublishTime,
    ApplicationId,
    SubscriptionId,
    NotificationType,
    NotificationVersion,
    ASIN
FROM flatten_data

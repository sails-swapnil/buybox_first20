{% snapshot some_snapshots %}

{{
    config(
        target_database='BUYBOX',
        target_schema='DEV',
        unique_key='s_key',
        strategy='check',
        check_cols=['s_key', 'MarketplaceId']
    )
}}

SELECT 
    s_key, 
    ASIN, 
    MarketplaceId
FROM {{ ref('src_products') }}

{% endsnapshot %}
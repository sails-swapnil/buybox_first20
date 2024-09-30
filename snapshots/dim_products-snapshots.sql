{% snapshot dim_products-snapshots %}

{{
    config(
        target_schema='snapshots',
        unique_key='ASIN',
        strategy='check',
        check_cols=['swa']
    )
}}

  select * FROM {{ ref('dim_products') }}

{% endsnapshot %}
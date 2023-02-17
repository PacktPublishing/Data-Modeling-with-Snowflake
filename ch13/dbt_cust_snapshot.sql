{% snapshot dim_cust_snapshot %}

{{
    config(
        target_schema="ch13_dims",
        strategy="timestamp",
        unique_key="customer_id",
        updated_at="__ldts",
    )
}}

with
    source as (
        select
            customer_id,
            name,
            address,
            location_id,
            phone,
            account_balance_usd,
            market_segment,
            comment, 
            __ldts,
            md5(account_balance_usd)
        from {{ source("sflkbook", "src_customer") }}

    )

select *
from source
where true

{% endsnapshot %}

/* step 1 */
create or replace temporary table "DIM_CUST_SNAPSHOT__dbt_tmp"  as
        (with snapshot_query as (

        



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
            comment,  -- hash type 2 attributes for easy compare
            __ldts,
            md5(account_balance_usd)
        from src_customer

    )

select *
from source
where true


    ),

    snapshotted_data as (

        select *,
            customer_id as dbt_unique_key

        from "DIM_CUST_SNAPSHOT"
        where dbt_valid_to is null

    ),

    insertions_source_data as (

        select
            *,
            customer_id as dbt_unique_key,
            __ldts as dbt_updated_at,
            __ldts as dbt_valid_from,
            nullif(__ldts, __ldts) as dbt_valid_to,
            md5(coalesce(cast(customer_id as varchar ), '')
         || '|' || coalesce(cast(__ldts as varchar ), '')
        ) as dbt_scd_id

        from snapshot_query
    ),

    updates_source_data as (

        select
            *,
            customer_id as dbt_unique_key,
            __ldts as dbt_updated_at,
            __ldts as dbt_valid_from,
            __ldts as dbt_valid_to

        from snapshot_query
    ),

    insertions as (

        select
            'insert' as dbt_change_type,
            source_data.*

        from insertions_source_data as source_data
        left outer join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where snapshotted_data.dbt_unique_key is null
           or (
                snapshotted_data.dbt_unique_key is not null
            and (
                (snapshotted_data.dbt_valid_from < source_data.__ldts)
            )
        )

    ),

    updates as (

        select
            'update' as dbt_change_type,
            source_data.*,
            snapshotted_data.dbt_scd_id

        from updates_source_data as source_data
        join snapshotted_data on snapshotted_data.dbt_unique_key = source_data.dbt_unique_key
        where (
            (snapshotted_data.dbt_valid_from < source_data.__ldts)
        )
    )

    select * from insertions
    union all
    select * from updates

        );
		
		
/* step 2 */

merge into "DIM_CUST_SNAPSHOT" as DBT_INTERNAL_DEST
    using "DIM_CUST_SNAPSHOT__dbt_tmp" as DBT_INTERNAL_SOURCE
    on DBT_INTERNAL_SOURCE.dbt_scd_id = DBT_INTERNAL_DEST.dbt_scd_id

    when matched
     and DBT_INTERNAL_DEST.dbt_valid_to is null
     and DBT_INTERNAL_SOURCE.dbt_change_type in ('update', 'delete')
        then update
        set dbt_valid_to = DBT_INTERNAL_SOURCE.dbt_valid_to

    when not matched
     and DBT_INTERNAL_SOURCE.dbt_change_type = 'insert'
        then insert ("CUSTOMER_ID", "NAME", "ADDRESS", "LOCATION_ID", "PHONE", "ACCOUNT_BALANCE_USD", "MARKET_SEGMENT", "COMMENT", "__LDTS", "MD5(ACCOUNT_BALANCE_USD)", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")
        values ("CUSTOMER_ID", "NAME", "ADDRESS", "LOCATION_ID", "PHONE", "ACCOUNT_BALANCE_USD", "MARKET_SEGMENT", "COMMENT", "__LDTS", "MD5(ACCOUNT_BALANCE_USD)", "DBT_UPDATED_AT", "DBT_VALID_FROM", "DBT_VALID_TO", "DBT_SCD_ID")

;

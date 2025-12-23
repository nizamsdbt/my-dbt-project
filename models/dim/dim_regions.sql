-- models/dim_regions.sql

{{
    config(
        materialized='incremental',
        unique_key = 'region_id',
        incremental_strategy = 'delete+insert',
        tags = ['dim']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['region_id']) }} as region_id_sk,
    region_id,
    INITCAP(region_name) AS region_name,
    CURRENT_TIMESTAMP AS load_time
FROM {{ ref('stg_regions') }}

{% if is_incremental() %}

{{ inc() }}

{% endif %}
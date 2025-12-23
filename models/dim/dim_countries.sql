-- models/dim_countries.sql

{{
    config(
        materialized='incremental',
        unique_key = 'country_id',
        incremental_strategy = 'delete+insert',
        tags = ['dim']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['country_id']) }} as country_id_sk,
    {{ dbt_utils.generate_surrogate_key(['region_id']) }} as region_id_sk,
    country_id,
    INITCAP(country_name) AS country_name,
    region_id,
    CURRENT_TIMESTAMP AS load_time
FROM {{ ref('stg_countries') }}

{% if is_incremental() %}

{{ inc() }}

{% endif %}
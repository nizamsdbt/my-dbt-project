-- models/dim_locations.sql

{{
    config(
        materialized='incremental',
        unique_key = 'location_id',
        incremental_strategy = 'delete+insert',
        tags = ['dim']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_id_sk,
    {{ dbt_utils.generate_surrogate_key(['country_id']) }} as country_id_sk,
    location_id,
    INITCAP(city) AS city,
    INITCAP(state_province) AS state,
    country_id,
    CONCAT(city, ', ', state_province) AS full_location,
    CURRENT_TIMESTAMP AS load_time
FROM {{ ref('stg_locations') }}

{% if is_incremental() %}

{{ inc() }}

{% endif %}
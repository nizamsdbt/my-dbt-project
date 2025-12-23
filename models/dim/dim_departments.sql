-- models/dim_departments.sql

{{
    config(
        materialized='incremental',
        unique_key = 'department_id',
        incremental_strategy = 'delete+insert',
        tags = ['dim']
    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['department_id']) }} as department_id_sk,
    {{ dbt_utils.generate_surrogate_key(['location_id']) }} as location_id_sk,
    department_id,
    INITCAP(department_name) AS department_name,
    manager_id,
    location_id,
    load_time as source_load_time,
	CURRENT_TIMESTAMP AS load_time
FROM {{ ref('stg_departments') }}

{% if is_incremental() %}

{{ inc() }}

{% endif %}
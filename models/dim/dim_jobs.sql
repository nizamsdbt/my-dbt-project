-- models/dim_jobs.sql

{{
    config(
        materialized='incremental',
        unique_key = 'job_id',
        incremental_strategy = 'delete+insert',
        tags = ['dim']

    )
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['job_id']) }} as job_id_sk,
    job_id,
    INITCAP(job_title) AS job_title,
    min_salary,
    max_salary,
    CURRENT_TIMESTAMP AS load_time
FROM {{ ref('stg_jobs') }}

{% if is_incremental() %}

{{ inc() }}

{% endif %}
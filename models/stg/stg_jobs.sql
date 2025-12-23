-- models/stg_jobs.sql
{{
	config(
		materialized = 'table',
		tags = ['stg']
	)
}}

SELECT
    job_id,
    job_title,
    min_salary,
    max_salary,
    load_time as source_load_time,
    CURRENT_TIMESTAMP AS load_time
FROM {{ source('hr','src_jobs') }}
WHERE job_id IS NOT NULL
-- models/stg_salary.sql
{{
	config(
		materialized = 'table',
		tags = ['stg']
	)
}}

SELECT
    {{ dbt_utils.generate_surrogate_key(['employee_id']) }} as employee_id_sk,    
	employee_id,
	first_name,
	last_name,
	job_title,
	department_name,
	city,
	country_name,
	region_name,
	salary_date,
	hra,
	allowances,
	pf,
    load_time as source_load_time,
	current_timestamp as LOAD_TIME
FROM {{ source('hr','src_salary') }}
WHERE employee_id IS NOT NULL
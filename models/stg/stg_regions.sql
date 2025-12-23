-- models/stg_regions.sql
{{
	config(
		materialized = 'table',
		tags = ['stg']
	)
}}

SELECT
    region_id,
    INITCAP(region_name) AS region_name,
    load_time as source_load_time,
    CURRENT_TIMESTAMP AS load_time
FROM  {{ source('hr','src_regions') }}
WHERE region_id IS NOT NULL

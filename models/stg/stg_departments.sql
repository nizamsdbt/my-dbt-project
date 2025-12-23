{{
    config(
        materialized = 'table',
        tags = ['stg']
    )
}}

select 
DEPARTMENT_ID,
DEPARTMENT_NAME,
MANAGER_ID,
LOCATION_ID,
LOAD_TIME as source_load_time,
current_timestamp as LOAD_TIME
from {{ source('hr','src_departments') }}
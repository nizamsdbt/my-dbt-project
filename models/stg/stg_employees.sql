{{
    config(
        materialized = 'table',
        tags = ['stg']
    )
}}

select 
EMPLOYEE_ID,
FIRST_NAME,
LAST_NAME,
EMAIL,
PHONE_NUMBER,
HIRE_DATE,
JOB_ID,
SALARY,
COMMISSION_PCT,
MANAGER_ID,
DEPARTMENT_ID,
LOAD_TIME as source_load_time,
current_timestamp as LOAD_TIME
from {{ source('hr','src_employees') }}
where SALARY is not null
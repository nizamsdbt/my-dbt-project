{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'employee_id',
        tags = ['dim']
    )
}}

select 
{{ dbt_utils.generate_surrogate_key(['EMPLOYEE_ID']) }} as EMPLOYEE_ID_sk,
{{ dbt_utils.generate_surrogate_key(['JOB_ID']) }} as JOB_ID_sk,
{{ dbt_utils.generate_surrogate_key(['DEPARTMENT_ID']) }} as DEPARTMENT_ID_sk,
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
LOAD_TIME as SOURCE_LOAD_TIME,
current_timestamp as LOAD_TIME
from {{ ref('stg_employees') }}

{% if is_incremental() %}

{{ inc() }}

{% endif %}
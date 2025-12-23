{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'delete+insert',
        unique_key = 'employee_id',
        tags = ['fct']
    )
}}

{% set max_load_time = "( select coalesce(max('load_time'),'1900-01-01') from {{this}})" %}

select
emp.EMPLOYEE_ID,
job.job_id,
dept.DEPARTMENT_ID,
loc.LOCATION_ID,
con.COUNTRY_id,
reg.REGION_id,
salary.SALARY_DATE,
emp.salary as basic_salary,
salary.HRA,
salary.ALLOWANCES,
salary.PF,
(emp.salary+salary.HRA+salary.ALLOWANCES+salary.PF) as gross_salary,
(emp.salary+salary.HRA+salary.ALLOWANCES) as net_salary,
GREATEST_IGNORE_NULLS(
salary.LOAD_TIME,
emp.LOAD_TIME,
job.LOAD_TIME,
dept.LOAD_TIME,
loc.LOAD_TIME,
con.LOAD_TIME,
reg.LOAD_TIME
) as SOURCE_LOAD_TIME,

current_timestamp as LOAD_TIME

from {{ ref('stg_salary') }} as salary

inner join {{ref('dim_employees')}} as emp
on salary.employee_id_sk = emp.employee_id_sk

inner join {{ref('dim_jobs')}} as job
on emp.job_id_sk = job.job_id_sk

inner join {{ref('dim_departments')}} as dept
on emp.department_id_sk = dept.department_id_sk

inner join {{ref('dim_locations')}} as loc
on loc.location_id_sk  = dept.location_id_sk

inner join {{ref('dim_countries')}} as con
on loc.country_id_sk = con.country_id_sk

inner join {{ref('dim_regions')}} as reg
on con.region_id_sk = reg.region_id_sk

{% if is_incremental() %}

where salary.SOURCE_LOAD_TIME > {{ max_load_time }}

or emp.SOURCE_LOAD_TIME > {{ max_load_time }}
 
or job.SOURCE_LOAD_TIME > {{ max_load_time }}

or dept.SOURCE_LOAD_TIME > {{ max_load_time }}

or loc.SOURCE_LOAD_TIME > {{ max_load_time }}

or con.SOURCE_LOAD_TIME > {{ max_load_time }}

or reg.SOURCE_LOAD_TIME > {{ max_load_time }}

{% endif %}
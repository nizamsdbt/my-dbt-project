{{
    config(materialized = 'table')
}}

select * from SOURCE.HR.EMPLOYEES
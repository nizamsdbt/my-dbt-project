{% macro inc() %}

where SOURCE_LOAD_TIME >= (
    select coalesce(max(LOAD_TIME),'1900-01-01')
    from {{this}}
)

{% endmacro %}
WITH dim_funcionarios as (
    SELECT *
    FROM {{ref('int_vendas__join_funcionarios')}}
)

SELECT *
FROM dim_funcionarios
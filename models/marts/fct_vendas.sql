WITH fct_vendas as (
    SELECT *
    FROM {{ref('int_vendas__metricas_vendas')}}
)

SELECT *
FROM fct_vendas
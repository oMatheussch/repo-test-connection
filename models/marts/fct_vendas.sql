WITH fct_vendas AS (
    SELECT *
    FROM {{ ref('int_vendas__metricas_vendas') }}
)

SELECT *
FROM fct_vendas
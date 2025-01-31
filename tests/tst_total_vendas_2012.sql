/*
    Esse teste garante que os dados de vendas brutas
    de 2012, estão corretos de acordo com o valor auditado
    da contabilidade de: R$ 226.298,50 
*/

WITH vendas_2012 AS (
    SELECT 
        sum(total_bruto) as soma_total_bruto
    FROM {{ ref('int_vendas__metricas_vendas') }}
    WHERE data_pedido between '2012-01-01' and '2012-12-31'
)

SELECT *
FROM vendas_2012
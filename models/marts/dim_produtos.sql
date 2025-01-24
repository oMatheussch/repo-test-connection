WITH dim_produtos as (
    SELECT *
    FROM {{ref('int_vendas__prep_produtos')}}
)

SELECT *
FROM dim_produtos
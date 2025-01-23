WITH categorias as (
    SELECT *
    from {{ __ref('stg_erp__categorias') }}
)

, produtos as (
    SELECT *
    FROM {{ __ref('stg_erp__produtos')}}
)

, fornecedores as (
    SELECT *
    FROM {{ __ref('stg_erp__fornecedores')}}
)

, juncao as (
    SELECT 
        codigo_produto
    FROM produtos
    LEFT JOIN categorias
        ON produtos.codigo_categoria = categorias.codigo_categoria 
    LEFT JOIN fornecedores
        ON produtos.codigo_fornecedor = fornecedores.codigo_fornecedor
)
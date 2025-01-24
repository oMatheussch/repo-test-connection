WITH categorias as (
    SELECT *
    from {{ ref('stg_erp__categorias') }}
)

, produtos as (
    SELECT *
    FROM {{ ref('stg_erp__produtos')}}
)

, fornecedores as (
    SELECT *
    FROM {{ ref('stg_erp__fornecedores')}}
)

, juncao as (
    SELECT 
        produtos.codigo_produto
        , produtos.nome_produto
        , produtos.quantidade_por_unidade
        , produtos.preco_por_unidade
        , produtos.unidades_em_estoque
        , produtos.unidades_por_pedidos
        , produtos.nivel_de_pedido
        , produtos.produto_descontinuado
        , categorias.nome_categoria
        , fornecedores.nome_fornecedor
        , fornecedores.cidade_fornecedor
        , fornecedores.pais_fornecedor
    FROM produtos
    LEFT JOIN categorias
        ON produtos.codigo_categoria = categorias.codigo_categoria 
    LEFT JOIN fornecedores
        ON produtos.codigo_fornecedor = fornecedores.codigo_fornecedor
)

SELECT *
FROM juncao
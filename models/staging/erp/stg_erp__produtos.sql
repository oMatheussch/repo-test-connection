WITH row_data AS (
    SELECT *
    FROM {{ source('erp', 'products')}}
)

, enrichment_data AS (
    SELECT 
        id as codigo_produto
        , productname as nome_produto
        , supplierid as codigo_fornecedor
        , categoryid as codigo_categoria
        , quantityperunit as preco_por_unidade
        , unitsinstock as unidades_em_estoque
        , unitsonorder as unidades_por_pedidos
        , reorderlevel as nivel_de_pedido
        , discontinued as produto_descontinuado
    FROM {{ source('erp', 'products')}}
)

SELECT *
FROM enrichment_data
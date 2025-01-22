WITH row_data AS (
    SELECT 
        id as codigo_produto
        , productname as nome_produto
        , supplierid as codigo_fornecedor
        , categoryid as codigo_categoria
        , quantityperunit as quantidade_por_unidade
        , unitsinstock as unidades_em_estoque
        , unitsonorder as unidades_em_pedidos
        , reorderlevel as nivel_de_reordenamento
        , discontinued as produto_descontinuado
    FROM {{ source('erp', 'products')}}
)
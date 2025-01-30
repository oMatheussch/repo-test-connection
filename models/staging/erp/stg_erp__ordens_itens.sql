WITH row_data AS (
    SELECT *
    FROM {{ source('erp', 'order_detail')}}
)

, enrichment_data AS (
    SELECT 
        cast(ID as int) as codigo_pedido_produto
	    , cast(ORDERID as int ) as codigo_pedido
	    , cast(PRODUCTID as int) as codigo_produto
	    , cast(UNITPRICE as float) as preco_unitario
	    , cast(QUANTITY as float) as quantidade 
	    , cast(DISCOUNT as float) as disconto
    FROM {{ source('erp', 'order_detail')}}
)

SELECT *
FROM enrichment_data
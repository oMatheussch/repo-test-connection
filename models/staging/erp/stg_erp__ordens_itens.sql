WITH row_data AS (
    SELECT *
    FROM {{ source('erp', 'order_details')}}
)

, enrichment_data AS (
    SELECT 
        cast(ID as string) as codigo_pedido_produto
	    , cast(ORDERID as int ) as codigo_pedido
	    , cast(PRODUCTID as int) as codigo_produto
	    , cast(UNITPRICE as float) as preco_unitario
	    , cast(QUANTITY as float) as quantidade 
	    , cast(DISCOUNT as float) as desconto
    FROM {{ source('erp', 'order_details')}}
)

SELECT *
FROM enrichment_data
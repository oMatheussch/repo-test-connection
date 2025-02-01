WITH row_data AS (
    SELECT *
    FROM {{ source('erp', 'orders')}}
)

, enrichment_data AS (
    SELECT 
        cast(ID as int) as codigo_pedido
	    , cast(CUSTOMERID as string) as codigo_cliente 
	    , cast(EMPLOYEEID as int) as codigo_funcionario
	    , cast(ORDERDATE as date) as data_pedido
	    , cast(REQUIREDDATE as date) as data_entrega_prevista
	    , cast(SHIPPEDDATE as date) as data_envio
	    , cast(SHIPVIA as varchar) as metodo_envio
	    , cast(FREIGHT as varchar) as frete
	    , cast(SHIPNAME as varchar) as nome_destinatario
	    , cast(SHIPADDRESS as varchar) as endereco_destinatario
	    , cast(SHIPCITY as varchar) as cidade_destinatario
	    , cast(SHIPREGION as varchar) as regiao_destinatario
	    , cast(SHIPPOSTALCODE as varchar) as codigo_postal_destinatario
	    , cast(SHIPCOUNTRY as varchar) as pais_destinatario
    FROM {{ source('erp', 'orders')}}
)

SELECT *
FROM enrichment_data
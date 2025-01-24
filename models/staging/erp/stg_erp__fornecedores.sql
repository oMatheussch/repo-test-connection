WITH raw_data AS (
    SELECT *
    FROM {{ source('erp', 'suppliers')}}
)

, enrichment_data AS (
    SELECT 
        ID as codigo_fornecedor
        , companyname as nome_fornecedor
        , contactname as nome_do_contato
        , contacttitle as titulo_do_contato
        , CONTACTTITLE
        , address as endereco_fornecedor
        , city as cidade_fornecedor
        , region as regiao
        , postalcode as codigo_postal
        , country as pais_fornecedor
        , phone as telefone
        , fax as fax
        , homepage as pagina_inicial
    FROM raw_data
)

SELECT *
FROM enrichment_data
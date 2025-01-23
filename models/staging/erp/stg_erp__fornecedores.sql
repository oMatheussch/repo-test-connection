WITH raw_data AS (
    SELECT *
    FROM {{ source('erp', 'suppliers')}}
)

, enrichment_data AS (
    SELECT 
        id as codigo_fornecedor
        , companyname as nome_compania
        , contactname as nome_do_contato
        , contacttitle as titulo_do_contato
        , address as endereco
        , city as cidade
        , region as regiao
        , postalcode as codigo_postal
        , country as pais
        , phone as telefone
        , fax as fax
        , homepage as pagina_inicial
)

SELECT *
FROM enrichment_data
WITH row_data AS (
    SELECT 
        *
    FROM {{ source('erp', 'category')}}
),

enrichment_data AS (
    SELECT 
        id as codigo_categoria
        , categoryname as nome_categoria
        , description as descricao_categoria
    FROM row_data
)

SELECT *
FROM enrichment_data

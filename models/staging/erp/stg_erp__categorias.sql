WITH row_data AS (
    SELECT 
        *
    FROM {{ source('erp', 'category')}}
),

rename_data AS (
    SELECT 
        id as codigo_categoria
        , categoryname as nome_categoria
        , description as descricao_categoria
    FROM row_data
)

SELECT *
FROM rename_data

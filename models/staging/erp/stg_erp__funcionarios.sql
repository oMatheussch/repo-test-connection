WITH raw_data AS (
    SELECT *
    FROM {{ source('erp', 'employees')}}
)

, enrichment_data AS (
    SELECT 
        ID as codigo_funcionario
	    , concat(FIRSTNAME, ' ', LASTNAME) as nome_funcionario
	    , cast(TITLE as varchar) as cargo_funcionario
	    , cast(REPORTSTO as varchar) as codigo_gerente
        , cast(BIRTHDATE as varchar) as data_nascimento
        , cast(HIREDATE as varchar) as data_contratacao
        , cast(CITY as varchar) as cidade_funcionario
        , cast(REGION as varchar) as regiao_funcionario
        , cast(COUNTRY as varchar) pais_funcionario
        --, cast(TITLEOFCOURTESY as varchar)
	    --, cast(ADDRESS as varchar)  
	    --, cast(POSTALCODE as varchar)
	    --, cast(HOMEPHONE as varchar)
	    --, cast(EXTENSION as varchar)
	    --, cast(PHOTO as varchar)
	    --, cast(NOTES as varchar)
	    --, cast(PHOTOPATH as varchar)
    FROM raw_data
)

SELECT *
FROM enrichment_data
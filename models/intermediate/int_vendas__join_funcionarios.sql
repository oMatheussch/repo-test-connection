WITH funcionarios as (
    SELECT *
    from {{ ref('stg_erp__funcionarios') }}
)

, join_funcionarios as (
    SELECT 
        funcionarios.codigo_funcionario
	    , funcionarios.nome_funcionario
	    , funcionarios.cargo_funcionario
        , gerentes.nome_funcionario as nome_gerente
        , funcionarios.data_nascimento
        , funcionarios.data_contratacao
        , funcionarios.cidade_funcionario
        , funcionarios.regiao_funcionario
        , funcionarios.pais_funcionario
    FROM funcionarios
    LEFT JOIN funcionarios as gerentes
         ON funcionarios.codigo_funcionario = gerentes.codigo_funcionario
)

SELECT *
FROM join_funcionarios
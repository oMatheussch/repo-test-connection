WITH ordens as (
    SELECT *
    from {{ ref('stg_erp__ordens') }}
)

, ordens_itens as (
    SELECT *
    from {{ ref('stg_erp__ordens_itens') }}
)

, join_ordens as (
    SELECT 
        stg_erp__ordens.codigo_pedido
        , stg_erp__ordens_itens.codigo_produto
        , stg_erp__ordens_itens.codigo_pedido_produto
        , stg_erp__ordens_itens.preco_unitario
        , stg_erp__ordens_itens.quantidade
        , stg_erp__ordens_itens.desconto
        , case 
            when stg_erp__ordens_itens.desconto > 0 then true
            else false
        end as desconto_aplicado
        , stg_erp__ordens_itens.quantidade * stg_erp__ordens_itens.preco_unitario as total_bruto
        , (stg_erp__ordens_itens.preco_unitario - stg_erp__ordens_itens.desconto) * stg_erp__ordens_itens.quantidade as total_liquido
        , stg_erp__ordens.codigo_cliente
        , stg_erp__ordens.codigo_funcionario
        , stg_erp__ordens.data_pedido
        , stg_erp__ordens.data_entrega_prevista
        , stg_erp__ordens.data_envio
        , case
            when data_envio > data_entrega_prevista then true
            else false
        end as pedido_atrasado
        , stg_erp__ordens.metodo_envio
        , stg_erp__ordens.frete / (count(*) over (partition by stg_erp__ordens.codigo_pedido)) as frete_rateado
        , stg_erp__ordens.nome_destinatario
        , stg_erp__ordens.endereco_destinatario
        , stg_erp__ordens.cidade_destinatario
        , stg_erp__ordens.regiao_destinatario
        , stg_erp__ordens.pais_destinatario

    FROM stg_erp__ordens_itens
    INNER JOIN stg_erp__ordens
        ON stg_erp__ordens_itens.codigo_pedido = stg_erp__ordens.codigo_pedido
) 

SELECT *
FROM join_ordens
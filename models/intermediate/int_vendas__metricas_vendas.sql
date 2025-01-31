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
        ordens.codigo_pedido
        , ordens_itens.codigo_produto
        , ordens_itens.codigo_pedido_produto
        , ordens_itens.preco_unitario
        , ordens_itens.quantidade
        , ordens_itens.desconto
        , case 
            when ordens_itens.desconto > 0 then true
            else false
        end as desconto_aplicado
        , ordens_itens.quantidade * ordens_itens.preco_unitario as total_bruto
        , (ordens_itens.preco_unitario - ordens_itens.desconto) * ordens_itens.quantidade as total_liquido
        , ordens.codigo_cliente
        , ordens.codigo_funcionario
        , ordens.data_pedido
        , ordens.data_entrega_prevista
        , ordens.data_envio
        , case
            when data_envio > data_entrega_prevista then true
            else false
        end as pedido_atrasado
        , ordens.metodo_envio
        , ordens.frete / (count(*) over (partition by ordens.codigo_pedido)) as frete_rateado
        , ordens.nome_destinatario
        , ordens.endereco_destinatario
        , ordens.cidade_destinatario
        , ordens.regiao_destinatario
        , ordens.pais_destinatario

    FROM ordens_itens
    INNER JOIN ordens
        ON ordens_itens.codigo_pedido = ordens.codigo_pedido
) 

SELECT *
FROM join_ordens
WITH cte_programas AS (
  SELECT 
    TRIM(n."Codigo Produto") AS produto_id,
    n."Programa Produto" AS programa
  FROM integration_bu_secad.legado_products n
),
ultimo_produto AS (
  SELECT DISTINCT ON (mb.cliente)
    mb.cliente AS user_id,
    pg.programa,
    mb.valor_total,
    mb.data_emissao
  FROM integration_bu_secad.movimentacoes_bancarias mb
  LEFT JOIN cte_programas pg
    ON LPAD(mb.cod_produto, 9, '0') = pg.produto_id
  WHERE 
    mb.data_vencto >= current_date - INTERVAL '60 days'
    AND mb.data_baixa IS NOT NULL
    AND mb.tipo_produto != 'curso'
  ORDER BY mb.cliente, mb.data_emissao DESC
)
SELECT 
    cbd.program,
    cbd.expertise,
    cbd.area,
    cbd.invoice_type,
    cbd.payment_type,
    up.valor_total AS value,
    cbd.start_date,
    cbd.end_date,
    su.cpf,
    su.email,
    su.city,
    su.address,
    su.state,
    CONCAT(su.ddd, su.phone) AS phone,
    CONCAT(su.ddd, su.phone_alt) AS phone_alt,
    order_id,
    cbd.user_id,
    cbd.user AS name,
    cbd.user_birth_date AS birth_date,
    cbd.user_age
FROM 
    bu_secad.current_base_detailed cbd
LEFT JOIN 
    ultimo_produto up ON cbd.user_id = up.user_id
LEFT JOIN 
    bu_secad.users su ON cbd.user_id = su.user_id
WHERE 
    end_date >= current_date + INTERVAL '2 months'
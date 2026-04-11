WITH Sispag AS (
    SELECT 
        vd_produto.nomeresumido,
        TO_CHAR(DATE_TRUNC('hour', vd_compra.datahora), 'HH24:MI') AS hr,
        vd_produto.nomeresumido::text as nome,
        CASE 
            WHEN programs.area ~* 'Mental' THEN 'Psicologia'
            WHEN programs.area IS NOT NULL THEN programs.area
            ELSE CASE 
                WHEN vd_produto.nomeresumido::text = ANY(ARRAY['PROURGEM','PROENDÓCRINO','PRO-UROLOGIA','PROTERAPÊUTICA']) THEN 'Medicina'
                WHEN vd_produto.nomeresumido::text = ANY(ARRAY['PROEMED','PRORN','PRONEUROPED']) THEN 'Pediatria'
                WHEN vd_produto.nomeresumido::text = ANY(ARRAY['PROPSIQ','PROPSICOMED']) THEN 'Psiquiatria'
                WHEN vd_produto.nomeresumido::text = 'PROMEVET' THEN 'Veterinária'
                WHEN vd_produto.nomeresumido::text = 'PRONUTRI' THEN 'Nutrição'
                WHEN vd_produto.nomeresumido::text ~* 'PROENF' THEN 'Enfermagem'
                WHEN vd_produto.nomeresumido::text ~* 'PROFISIO' THEN 'Fisioterapia'
                WHEN vd_produto.nomeresumido::text = ANY(ARRAY['PRONEUROPSI','PROPSICO','PROCOGNITIVA']) THEN 'Psicologia'
                ELSE 'Outras Áreas' END
        END AS area,
        vd_compraitem.valor * vd_compra.valortotal / NULLIF(SUM(vd_compraitem.valor) OVER (PARTITION BY vd_compra.compraid), 0) AS value,
            CASE
        WHEN vd_compra."codVendedor" IS NULL THEN 'eCommerce'
        WHEN LEFT(vd_compra."codVendedor"::text, 1) = '8' THEN 'Representantes'
        WHEN vd_compra."codVendedor"::text IN ('9186', '9323', '9326', '9325') THEN 'Receptivo'
        WHEN LEFT(vd_compra."codVendedor"::text, 1) = 'R' THEN 'Renovacao'
        ELSE 'Call Center'
    END AS channel,
    
        CASE
        WHEN vd_compra."codVendedor" IS NULL THEN DATE(timezone('America/Sao_Paulo'::text, timezone('UTC'::text, vd_compra.datahora)))
        WHEN vd_compra."codVendedor"::text = '8000'::text THEN DATE(timezone('America/Sao_Paulo'::text, timezone('UTC'::text, vd_compra.datahora)))
        ELSE DATE(vd_compra.datahora)
        
    END AS data
    FROM app_sispag_pagamento.vd_compra
    LEFT JOIN app_sispag_pagamento.vd_compraitem ON vd_compra.compraid = vd_compraitem.compraid
    LEFT JOIN app_sispag_pagamento.vd_produto ON vd_compraitem.produtoid = vd_produto.produtoid
    LEFT JOIN app_sispag_pagamento.vd_request ON vd_compra.requestid = vd_request.requestid
    LEFT JOIN app_sispag_pagamento.vd_cliente ON vd_compra.clienteid = vd_cliente.clienteid
    LEFT JOIN bu_secad.programs ON vd_produto.nomeresumido::text = programs.program
    WHERE DATE(vd_compra.datahora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') >= '2026-03-01'
    AND vd_produto.tipoproduto::text IN ('P')
    AND vd_request.ambiente::text = 'P'
    AND LOWER(vd_cliente.nome::text) NOT LIKE '%teste%'
)
SELECT 
    data,
    COUNT(*) FILTER (WHERE channel IN ('Call Center', 'Receptivo')) AS ven_cc,
    SUM(value) FILTER (WHERE channel IN ('Call Center', 'Receptivo')) AS receita_cc,
    COUNT(*) FILTER (WHERE channel IN ('Representantes')) AS ven_representantes,
    SUM(value) FILTER (WHERE channel IN ('Representantes')) AS receita_represantes,
    COUNT(*) FILTER (WHERE channel IN ('eCommerce')) AS ven_site,
    SUM(value) FILTER (WHERE channel IN ('eCommerce')) AS receita_site,
    COUNT(*) as Vendas_geral,
    SUM(value) AS Receita_Geral
FROM Sispag
GROUP BY data
ORDER BY data desc
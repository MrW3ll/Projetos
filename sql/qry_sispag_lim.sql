select
case
	when tipoproduto = 'C' then 'ARTMED360' else 'SECAD'
end as "nome_ies",
	right(replace(replace(replace(replace(app_sispag_pagamento.vd_cliente.celular,'(',''),')',''),'-',''),' ',''),11) "celular",
	vd_produto.tipoproduto::text,
	case
		when vd_compra."codVendedor" is null then date(timezone('America/Sao_Paulo'::text,
		timezone('UTC'::text,
		vd_compra.datahora)))
		when vd_compra."codVendedor"::text = '8000'::text then date(timezone('America/Sao_Paulo'::text,
		timezone('UTC'::text,
		vd_compra.datahora)))
		else date(vd_compra.datahora)
	end as dt,
	vd_compra.compraid as id,
	vd_compra.clienteid as user_id,
	vd_cliente.nome as name,
	lower(vd_cliente.email::text) as email,
	vd_cliente.cidade as city,
	vd_cliente."UF" as state,
	vd_compraitem.produtoid as product_id,
	case
		when vd_produto.nomeresumido::text = 'PROENDÓCRINO'::text then 'PROENDOCRINO'::character varying
		when vd_produto.nomeresumido::text = 'PROENF-URG'::text then 'PROENF/URG'::character varying
		when vd_produto.nomeresumido::text = 'PROFISIO/NEURO'::text then 'PROFISIO/NEF'::character varying
		when vd_produto.nomeresumido::text = 'PROFISIO/TRAUMA'::text then 'PROFISIO/TO'::character varying
		when vd_produto.nomeresumido::text = 'PROMEVET'::text then 'PROMEVET/PA'::character varying
		when vd_produto.nomeresumido::text = 'PROTERAPÊUTICA'::text then 'PROTERAPEUTICA'::character varying
		when vd_produto.nomeresumido::text = 'PROURGEM'::text then 'PROURGEN'::character varying
		when vd_produto.nomeresumido::text = 'PRO-UROLOGIA'::text then 'PROUROLOGIA'::character varying
		else vd_produto.nomeresumido
	end as program,
	case
		when programs.area is not null then programs.area
		else
            case
			when vd_produto.nomeresumido::text = any (array['PROURGEM'::character varying::text,
			'PROENDÓCRINO'::character varying::text,
			'PRO-UROLOGIA'::character varying::text,
			'PROTERAPÊUTICA'::character varying::text]) then 'Medicina'::text
			when vd_produto.nomeresumido::text = 'PROMEVET'::text then 'Veterinária'::text
			when vd_produto.nomeresumido::text = 'PROENF-URG'::text then 'Enfermagem'::text
			when vd_produto.nomeresumido::text = any (array['PROFISIO/NEURO'::character varying::text,
			'PROFISIO/TRAUMA'::character varying::text]) then 'Fisioterapia'::text
			else null::text
		end
	end as area,
	vd_compraitem.valor * vd_compra.valortotal / sum(vd_compraitem.valor) over (partition by vd_compra.compraid) as value,
	vd_compra.formapagamento as payment_type,
	vd_compra."codVendedor" as channel_id,
	case
		when vd_compra."codVendedor" is null then 'eCommerce'::text
		when "left"(vd_compra."codVendedor"::text,
		1) = '8'::text then 'Representantes'::text
		when "left"(vd_compra."codVendedor"::text,
		2) = any (array['90'::text,
		'93'::text]) then 'Receptivo'::text
		when "left"(vd_compra."codVendedor"::text,
		1) = 'R'::text then 'Renovacao'::text
		else 'Call Center'::text
	end as channel
	--vd_compra."valortotal" as valor_compra
from
	app_sispag_pagamento.vd_compra
left join app_sispag_pagamento.vd_compraitem on
	vd_compra.compraid = vd_compraitem.compraid
left join app_sispag_pagamento.vd_produto on
	vd_compraitem.produtoid = vd_produto.produtoid
left join app_sispag_pagamento.vd_request on
	vd_compra.requestid = vd_request.requestid
left join app_sispag_pagamento.vd_cliente on
	vd_compra.clienteid = vd_cliente.clienteid
left join bu_secad.programs on
	vd_produto.nomeresumido::text = programs.program
where
	date(timezone('America/Sao_Paulo'::text,
	timezone('UTC'::text,
	vd_compra.datahora))) >= current_date - interval '3 months'
	and vd_produto.tipoproduto::text in ('P', 'C')
	and vd_request.ambiente::text = 'P'::text
	and (vd_compra."codVendedor"::text <> '123'::text
		or vd_compra."codVendedor" is null)
	and lower(vd_cliente.nome::text) !~~ '%teste%'::text


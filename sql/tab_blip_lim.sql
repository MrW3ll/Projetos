with tab_blip as (
	SELECT 
		account_id,
		SUBSTRING(contact_id FROM 3 FOR 11) AS contact_id_trimmed,
		attendance_started_at::date as data_blip,
        ROW_NUMBER() OVER(PARTITION BY contact_id ORDER BY attendance_started_at desc) as rn,
		"template",
		tags,
		CASE 
		    WHEN tags = '' THEN 0
            WHEN tags ~* 'final de semana|transferencia|parou de interagir|abandono|tag|' THEN 0
		    WHEN tags ~* 'automatica|sem tag|metodo de pagamento|evento|bolsa gratuita' THEN 15
		    WHEN tags ~* 'semestre|sem interesse|sem tempo|analise de disciplinas' THEN 180
		    WHEN tags ~* 'matricula|aluno|financeiro|curso|metodologia|outra IES' THEN 30
            WHEN tags ~* 'parou de responder|negociacao' THEN 7
		    ELSE 10
		END AS retorno_em_dias,
		CASE
		    WHEN tags ~* 'numero errado|nao acionar|não relacionado|rel|fora do publico|engano' THEN 'Não_Retorno'
		    ELSE 'retornar'
		END AS status_retorno    
	FROM 
		integration_operations.blip_details_conversations
	WHERE 
		account_id ~* 'secad'
		AND attendance_started_at >= current_date - interval '6 months'
)

select contact_id_trimmed,data_blip,tags,retorno_em_dias,status_retorno
from tab_blip
where rn = 1
order by data_blip desc
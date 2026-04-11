
WITH ranked_calls AS (
    SELECT
        right(phone_number,11) as phone,
        customer_id,
        disposition_id,
        disposition_nivel_1,
        TO_CHAR(start_agent_date,'dd/mm/yyyy') as data_olos,
        ROW_NUMBER() OVER (
            PARTITION BY phone_number
            ORDER BY start_agent_date DESC
        ) AS rn,
        CASE 
            WHEN disposition_nivel_1 ~* 'agendado|outra plataforma' THEN 0
            WHEN disposition_nivel_1 ~* 'nao localizado|bolsa gratuita' THEN 15
            WHEN disposition_nivel_1 ~* 'matricula|financeiro|outra formacao|curso de interesse|outra ies|analise de disciplinas|metodologia' THEN 30
            WHEN disposition_nivel_1 ~* 'ligacao falhando|ligacao caiu|sem audio|nao informou|nao atende|apresentacao|metodo pagamento|intresse em evento' THEN 7
            WHEN disposition_nivel_1 ~* 'inadimplente' THEN 60
            WHEN disposition_nivel_1 ~* 'semestre|nao quer EAD|sem tempo|sem interesse' THEN 180
            ELSE 0
        END AS retorno_em_dias,
        CASE 
            WHEN disposition_nivel_1 ~* 'badnumber|fora do publico|nao acionar|desconhece|numero errado|sem afinidade|sem interesse' THEN 'Não retornar'
            ELSE 'retornar'
        END AS status_retorno    
        
    FROM integration_operations.vw_call_center_calls
    WHERE campaign_id IN (1025,1553,1605,1690)
    and start_agent_date >= current_date - interval '6 months'
    and extract(epoch from wrap_duration) > 0
)
SELECT
    phone,customer_id, disposition_nivel_1,data_olos,retorno_em_dias,status_retorno
FROM ranked_calls
WHERE rn = 1
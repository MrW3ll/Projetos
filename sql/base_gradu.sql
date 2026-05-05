WITH base_sales AS (
    SELECT
        CASE sk_product
            WHEN '14' THEN 'PUCPR'
            WHEN '11' THEN 'UCS'
            WHEN '19' THEN 'FAESA'
            WHEN '22' THEN 'UNIVALI'
            WHEN '34' THEN 'UNISAGRADO'
            WHEN '43' THEN 'EADUNISINOS'
        END AS ies,
        subscription_id AS cod_candidato,
        UPPER(name) AS nome,
        email,
        contact_id AS hubspotcontactid,
        cpf,
        RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 11) AS telefone,
        registration_at::DATE AS data_inscricao,
        campus AS polo,
        last_course_name AS curso,
        ingress_mode AS "tipo ingresso",
        CASE 
            WHEN subscription_status = 'Desclassificado' THEN 'Inscrito'
            ELSE subscription_status
        END AS status,
        last_activity_at::date AS "data do lead",
        last_ticket_interaction_at::date AS "data da tabulação",
        COALESCE(hsm_count, 0) AS qtd_hsm,
        CASE 
            WHEN last_ticket_tag ~* '(matricula|recusa|nao acionar|sem interesse|inadimplente|ja e aluno|telefone incorreto|sem afinidade)'
                THEN 'Não_acionar'
            ELSE last_ticket_tag
        END AS last_ticket_tag
    FROM mart_sales.rpt_phone_list_sales_ops_campaigns
    WHERE
        person_stage <> 'enrolled'
        AND subscription_status IN ('Avaliado','Desclassificado','Inscrito','Pré-Matriculado')
        AND is_high_school_graduate IS NOT FALSE
        AND registration_at >= DATE '2026-01-01'
),

calls_enriched AS (
    SELECT
        RIGHT(REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g'), 11) AS telefone,
        COUNT(*) OVER (PARTITION BY RIGHT(REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g'), 11)) AS qtd_call,
        FIRST_VALUE(disposition_nivel_1) OVER (
            PARTITION BY RIGHT(REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g'), 11)
            ORDER BY start_agent_date DESC
        ) AS last_tab_raw
    FROM integration_operations.vw_call_center_calls
    WHERE start_agent_date >= current_date - interval '1 month'
),

calls_final AS (
    SELECT DISTINCT
        telefone,
        qtd_call,
        CASE
            WHEN last_tab_raw ~* '(matricula|recusa|nao acionar|sem interesse|inadimplente|ja e aluno|telefone incorreto|sem afinidade)'
                THEN 'Não_acionar'
            ELSE last_tab_raw
        END AS last_tab
    FROM calls_enriched
)

SELECT
    b.*,
    c.qtd_call,
    c.last_tab
FROM base_sales b
LEFT JOIN calls_final c
    ON c.telefone = b.telefone
WHERE 
    b.last_ticket_tag IS DISTINCT FROM 'Não_acionar'
    AND b.ies IS NOT NULL;
WITH pivot_ies AS (
    SELECT *,
        CASE
            WHEN sk_product = '14' THEN 'PUCPR'
            WHEN sk_product = '11' THEN 'UCS'
            WHEN sk_product = '19' THEN 'FAESA'
            WHEN sk_product = '22' THEN 'UNIVALI'
            WHEN sk_product = '34' THEN 'UNISAGRADO'
            WHEN sk_product = '43' THEN 'EADUNISINOS'
            ELSE 'not_found'
        END AS ies
    FROM mart_sales.rpt_phone_list_sales_ops_campaigns
),
map_status AS (
    SELECT *,
        CASE
            WHEN last_ticket_tag ~* '(matricula|recusa|não acionar|sem interesse|ja e aluno|fora do publico|inadimplente|desconhece|sem afinidade)' THEN 0
            ELSE 1
        END AS flag_tab
    FROM pivot_ies
),
base_graduacao AS (
    SELECT ies,
        subscription_id AS Cód_candidato,
        UPPER(name) AS Nome,
        email,
        contact_id AS HubspotContactid,
        cpf,
        RIGHT(REGEXP_REPLACE(phone, '[^0-9]', '', 'g'), 11) AS telefone,
        registration_at::DATE AS "Data hora inscrição",
        campus AS Polo,
        last_course_name AS Curso,
        subscription_status AS Status,
        CASE
            WHEN hsm_count IS NULL THEN 0
            ELSE hsm_count
        END AS "Qtd. HSM",
        last_ticket_tag AS "Ultima Tabulação"
    FROM map_status
    WHERE person_stage NOT IN ('enrolled')
        AND flag_tab = 1
),
base AS (
    SELECT RIGHT(
            REGEXP_REPLACE(phone_number, '[^0-9]', '', 'g'),
            11
        ) AS telefone,
        disposition_nivel_1,
        start_agent_date
    FROM integration_operations.vw_call_center_calls
    WHERE start_agent_date >= current_date - interval '1 month'
),
ranked AS (
    SELECT *,
        ROW_NUMBER() OVER(
            PARTITION BY telefone
            ORDER BY start_agent_date DESC
        ) AS rn
    FROM base
),
calls_count AS (
    SELECT telefone,
        COUNT(*) AS qtd_call
    FROM base
    GROUP BY telefone
),
qtd_call AS (
    SELECT c.telefone,
        c.qtd_call,
        CASE
            WHEN r.disposition_nivel_1 ~* (
                'matricula|recusa|nao acionar|sem interesse|inadimplente|ja e aluno|telefone incorreto|sem afinidade'
            ) THEN 'Não_acionar'
            ELSE r.disposition_nivel_1
        END AS last_tab
    FROM calls_count c
        JOIN ranked r ON c.telefone = r.telefone
    WHERE r.rn = 1
)
SELECT bg.*,
    qc.qtd_call,
    qc.last_tab
FROM base_graduacao bg
    LEFT JOIN qtd_call qc ON qc.telefone = bg.telefone
WHERE bg.ies <> 'not_found'
    AND bg.status IS NOT NULL
    AND bg."Data hora inscrição" >= DATE '2026-01-01'
    AND bg."Qtd. HSM" <= 30
    AND qc.qtd_call <= 30
    AND qc.last_tab NOT IN ('Não_acionar')
WITH pivot_ies AS(
    SELECT *,
        CASE
            WHEN sk_product = '14' THEN 'PUCPR'
            WHEN sk_product = '11' THEN 'UCS'
            WHEN sk_product = '19' THEN 'FAESA'
            WHEN sk_product = '22' THEN 'UNIVALI'
            WHEN sk_product = '34' THEN 'UNISAGRADO'
            WHEN sk_product = '43' THEN 'UNISINOS EAD'
            ELSE 'not_found'
        END AS ies
    FROM mart_sales.rpt_phone_list_sales_ops_campaigns
),
map_status as (
    SELECT *,
        CASE
            WHEN last_ticket_tag ~* '(matricula|recusa|não acionar|sem interesse|ja e aluno|fora do publico|inadimplente|desconhece|sem afinidade)' THEN 0
            ELSE 1
        END AS flag_tab
    FROM pivot_ies
),
base_graduacao as (
    SELECT ies,
        subscription_id as Cód_candidato,
        UPPER(name) AS Nome,
        email,
        contact_id as HubspotContactid,
        cpf,
        RIGHT(
            REGEXP_REPLACE(phone, '[^0-9]', '', 'g')::text,
            11
        ) as Telefone,
        registration_at::DATE as "Data hora inscrição",
        campus as Polo,
        last_course_name as Curso,
        CASE
            WHEN ingress_mode = '100' THEN 'ENEM'
            WHEN ingress_mode = '75' THEN 'PROUNI'
            WHEN ingress_mode = '8' THEN 'Trans. Interna'
            WHEN ingress_mode = '9' THEN 'Transferencia'
            WHEN ingress_mode = 'A360 - FIN' THEN 'Artmed360 - Padrão - Online'
            WHEN ingress_mode = '74' THEN 'Vestibular Online'
            WHEN ingress_mode = 'A360-FI' THEN 'FORMA DE INGRESSO ONLINE'
            WHEN ingress_mode = '001' THEN 'Teste Forma Ingresso'
            WHEN ingress_mode = '11' THEN '2º Graduação (Ex Aluno)'
            WHEN ingress_mode = '11-1' THEN '2º Graduação'
            ELSE ingress_mode
        END as "Tipo Ingresso",
        subscription_status as Status,
        last_form_submission_at::DATE as "Data do lead",
        last_ticket_tag as "Ultima Tabulação",
        last_ticket_interaction_at::DATE as "Data da Tabulação",
        CASE
            WHEN hsm_count is NULL THEN 0
            ELSE hsm_count
        END AS "Qtd. HSM",
        last_hsm_template,
        last_hsm_sent_at::DATE as "Data ultimo HSM"
    FROM map_status
    WHERE person_stage NOT IN ('enrolled')
        AND flag_tab = 1
)
SELECT *
FROM base_graduacao
WHERE ies NOT IN ('not_found')
    AND status IS NOT NULL
    AND "Data hora inscrição" >= '2026-01-01'
    AND status IN ('Avaliado', 'Inscrito', 'Pré-matriculado')
    AND "Qtd. HSM" <= 13
WITH ranked_calls AS (
        SELECT
            RIGHT(phone_number,11) AS phone,

            ROW_NUMBER() OVER (
                PARTITION BY RIGHT(phone_number,11)
                ORDER BY start_agent_date DESC
            ) AS rn,
            campaign_id,
            tablename,
            CASE
                WHEN tablename ~* 'mental|psicologia' THEN 'Psicologia'
                WHEN tablename ~* 'multi' THEN 'Multi'
                WHEN tablename ~* 'fisio' THEN 'Fisioterapia'
                WHEN tablename ~* 'enf' THEN 'Enfermagem'
                WHEN tablename ~* 'medic' THEN 'Medicina'
                WHEN tablename ~* 'nutri' THEN 'Nutrição'
                WHEN tablename ~* 'vet' THEN 'Veterinária'
                WHEN tablename ~* 'ped' THEN 'Pediatria'
                WHEN tablename ~* 'psiquia' THEN 'Psiquiatria'
                ELSE 'Outras Áreas'
            END AS area,

            CASE
                WHEN campaign = '1690' THEN 'Chamada_manual'
                WHEN campaign = '1553' THEN 'receptivoWay'
                WHEN tablename ~* 'legado' THEN 'legado'
                WHEN tablename ~* 'evento' THEN 'evento'
                WHEN tablename ~* 'cancel' THEN 'cancelados'
                WHEN tablename ~* 'INATIV' THEN 'inativa'
                WHEN tablename ~* 'ATIV' THEN 'ativa'
                WHEN tablename ~* 'MATERIAL' THEN 'Material Rico'
                WHEN tablename ~* 'leads' THEN 'Base Leads'
                WHEN tablename ~* 'carri' THEN 'Carrinho'
                ELSE NULL
            END AS base_type,
            EXTRACT(HOUR FROM start_agent_date) AS hour,


            CASE
                WHEN EXTRACT(EPOCH FROM wrap_duration) > 0 THEN 1
                ELSE 0
            END AS atendida,

            DATE(start_agent_date) AS data

        FROM integration_operations.vw_call_center_calls
        WHERE campaign_id IN (1025,1553,1605,1690,1299)
        AND start_agent_date::date >= current_date - interval '5 months'
    )

    SELECT
        area,
        base_type,
        campaign_id,
        tablename,
        data,
        hour,
        COUNT(*) AS tentativas,
        SUM(atendida) AS atendidas
    FROM ranked_calls
    GROUP BY area, data,base_type,hour, tablename, campaign_id
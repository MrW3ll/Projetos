WITH cte_base AS (
    SELECT phone_number,
        disposition_id,
        disposition_nivel_1,
        end_agent_date,
        tablename,
        row_number() OVER (
            PARTITION BY phone_number
            ORDER BY start_agent_date DESC
        ) AS rn
    FROM integration_operations.vw_call_center_calls
    WHERE campaign_id IN (1025, 1553, 1605, 1690)
        AND start_agent_date >= CURRENT_DATE - INTERVAL '6 months'
),
cte_tags AS (
    SELECT phone_number,
        MAX(
            CASE
                WHEN rn = 1 THEN disposition_nivel_1
            END
        ) AS tag1,
        MAX(
            CASE
                WHEN rn = 1 THEN end_agent_date
            END
        ) AS data_tag1,
        MAX(
            CASE
                WHEN rn = 2 THEN disposition_nivel_1
            END
        ) AS tag2,
        MAX(
            CASE
                WHEN rn = 2 THEN end_agent_date
            END
        ) AS data_tag2,
        MAX(
            CASE
                WHEN rn = 3 THEN disposition_nivel_1
            END
        ) AS tag3,
        MAX(
            CASE
                WHEN rn = 3 THEN end_agent_date
            END
        ) AS data_tag3,
        MAX(
            CASE
                WHEN rn = 4 THEN disposition_nivel_1
            END
        ) AS tag4,
        MAX(
            CASE
                WHEN rn = 4 THEN end_agent_date
            END
        ) AS data_tag4,
        MAX(
            CASE
                WHEN rn = 5 THEN disposition_nivel_1
            END
        ) AS tag5,
        MAX(
            CASE
                WHEN rn = 5 THEN end_agent_date
            END
        ) AS data_tag5,
        MAX(
            CASE
                WHEN rn = 6 THEN disposition_nivel_1
            END
        ) AS tag6,
        MAX(
            CASE
                WHEN rn = 6 THEN end_agent_date
            END
        ) AS data_tag6,
        MAX(
            CASE
                WHEN rn = 7 THEN disposition_nivel_1
            END
        ) AS tag7,
        MAX(
            CASE
                WHEN rn = 7 THEN end_agent_date
            END
        ) AS data_tag7,
        MAX(
            CASE
                WHEN rn = 8 THEN disposition_nivel_1
            END
        ) AS tag8,
        MAX(
            CASE
                WHEN rn = 8 THEN end_agent_date
            END
        ) AS data_tag8,
        MAX(
            CASE
                WHEN rn = 9 THEN disposition_nivel_1
            END
        ) AS tag9,
        MAX(
            CASE
                WHEN rn = 9 THEN end_agent_date
            END
        ) AS data_tag9,
        MAX(
            CASE
                WHEN rn = 10 THEN disposition_nivel_1
            END
        ) AS tag10,
        MAX(
            CASE
                WHEN rn = 10 THEN end_agent_date
            END
        ) AS data_tag10
    FROM cte_base
    GROUP BY phone_number
),
cte2 as(
    SELECT *,
        (
            select count(*)
            from unnest(
                    array [tag1,tag2,tag3,tag4,tag5,tag6,tag7,tag8,tag9,tag10]
                ) as x(tag)
            where tag ~* 'BadNumber|Badline_qtd|CancelDial|NoAnswer'
        ) as fail_qtd,
        exists(
            select 1
            from unnest (
                    array [tag1,tag2,tag3,tag4,tag5,tag6,tag7,tag8,tag9,tag10]
                ) as x(tag)
            where tag ~* 'acionar|IMPRODUTIVO NAO ACIONAR|IMPRODUTIVO CAIXA POSTAL NAO ATENDE|IMPRODUTIVO RESPONDEU HSM SEM INTERESSE|HSM|SEM AFINIDADE|desconhece|IMPRODUTIVO Numero Errado|AFINIDADE|recusa'
        ) as nao_acionar
    FROM cte_tags
)
select phone_number --fail_qtd,
    --nao_acionar
from cte2
WHERE nao_acionar = true
    OR fail_qtd >= 7
order by data_tag1 DESC
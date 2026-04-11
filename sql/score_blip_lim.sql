WITH blip_bases AS (
    SELECT 
        RIGHT(phonenumber_clean, 11) AS phone_number,
        tag,
        closed_date,
        ROW_NUMBER() OVER (
            PARTITION BY RIGHT(phonenumber_clean,11) 
            ORDER BY started_date DESC
        ) AS rn
    FROM mart_operations.ft_blip_tickets
    WHERE ticket_router IN ('secadativo', 'secadativo2', 'secadreceptivo')
      AND started_date >= current_date - INTERVAL '6 months'
),

cte_tags AS (
    SELECT 
        phone_number,
        MAX(CASE WHEN rn = 1 THEN tag END)  AS tag1,
        MAX(CASE WHEN rn = 1 THEN closed_date END) AS data_tag1,
        MAX(CASE WHEN rn = 2 THEN tag END)  AS tag2,
        MAX(CASE WHEN rn = 2 THEN closed_date END) AS data_tag2,
        MAX(CASE WHEN rn = 3 THEN tag END)  AS tag3,
        MAX(CASE WHEN rn = 3 THEN closed_date END) AS data_tag3,
        MAX(CASE WHEN rn = 4 THEN tag END)  AS tag4,
        MAX(CASE WHEN rn = 4 THEN closed_date END) AS data_tag4,
        MAX(CASE WHEN rn = 5 THEN tag END)  AS tag5,
        MAX(CASE WHEN rn = 5 THEN closed_date END) AS data_tag5,
        MAX(CASE WHEN rn = 6 THEN tag END)  AS tag6,
        MAX(CASE WHEN rn = 6 THEN closed_date END) AS data_tag6,
        MAX(CASE WHEN rn = 7 THEN tag END)  AS tag7,
        MAX(CASE WHEN rn = 7 THEN closed_date END) AS data_tag7,
        MAX(CASE WHEN rn = 8 THEN tag END)  AS tag8,
        MAX(CASE WHEN rn = 8 THEN closed_date END) AS data_tag8,
        MAX(CASE WHEN rn = 9 THEN tag END)  AS tag9,
        MAX(CASE WHEN rn = 9 THEN closed_date END) AS data_tag9,
        MAX(CASE WHEN rn = 10 THEN tag END) AS tag10,
        MAX(CASE WHEN rn = 10 THEN closed_date END) AS data_tag10
    FROM blip_bases
    GROUP BY phone_number
),

cte2 AS (
    SELECT *,
        (
            SELECT COUNT(*)
            FROM unnest(ARRAY[tag1,tag2,tag3,tag4,tag5,tag6,tag7,tag8,tag9,tag10]) AS x(tag)
            WHERE tag ~* 'automatica|relacionado|cae'
        ) AS fail,

        EXISTS (
            SELECT 1
            FROM unnest(ARRAY[tag1,tag2,tag3,tag4,tag5,tag6,tag7,tag8,tag9,tag10]) AS x(tag)
            WHERE tag ~* 'recusa|acionar|interesse|errado'
        ) AS nao_acionar
    FROM cte_tags
)

SELECT 
    phone_number
    --fail,
    --nao_acionar
FROM cte2
WHERE 
    nao_acionar = TRUE
    OR fail >= 7
ORDER BY data_tag1 DESC;
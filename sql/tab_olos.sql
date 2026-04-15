SELECT count(*),
        disposition_nivel_1,
        call_type,
        TO_CHAR(start_agent_date,'MM/YYYY') as data
FROM integration_operations.vw_call_center_calls
WHERE campaign_id IN (1025,1553,1605,1690,1299)
AND start_agent_date::date >= '2025-04-01'
GROUP BY disposition_nivel_1,call_type,start_agent_date
SELECT  phone_number, COUNT(*) AS "TOTAL_CALLS"

FROM integration_operations.vw_call_center_calls

WHERE campaign_id IN ('1025', '1299', '1553', '1605')

AND start_agent_date >= current_date - interval '2 months'

GROUP BY disposition_nivel_1, phone_number
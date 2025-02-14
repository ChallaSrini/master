SELECT 
    user_id, 
    interest
FROM {{ ref('wrk_mart_explode_interests') }}
WHERE interest IN ('ML', 'AI')  
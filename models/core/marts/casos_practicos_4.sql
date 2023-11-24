---En promedio, ¿Cuántas sesiones únicas tenemos por hora?
    SELECT 
        AVG(unique_session) as avg_unique_session
    FROM (
        SELECT 
            DATE_TRUNC('hour', created_at) as horas,
            COUNT(distinct session_id) as unique_session 
        FROM {{ source('sql_server_dbo', 'events') }}
        GROUP BY 1
    )subquery


-- Processed Events Table (Base Event Store)
-- CREATE TABLE IF NOT EXISTS processed_events (
--     ip VARCHAR,
--     event_timestamp TIMESTAMP(3),
--     referrer VARCHAR,
--     host VARCHAR,
--     url VARCHAR,
--     geodata VARCHAR
-- );

-- Session Aggregated Events Table (Analytics)
-- CREATE TABLE IF NOT EXISTS session_events_aggregated (
--     event_hour TIMESTAMP(3),
--     ip VARCHAR,
--     host VARCHAR,
--     num_hits BIGINT
-- );


-- Latest 100 sessions
SELECT event_hour, ip, host, num_hits
FROM session_events_aggregated
ORDER BY event_hour DESC
LIMIT 100;

-- Average events per user session (overall)
SELECT AVG(num_hits) AS avg_events_per_session
FROM session_events_aggregated;

-- Average session activity by specific hosts
SELECT 
    host,
    AVG(num_hits) AS avg_events_per_session
FROM session_events_aggregated
WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY host
ORDER BY avg_events_per_session DESC;


-- Average sessions hits per day
SELECT DATE(event_hour) AS session_date, AVG(num_hits) AS avg_hits_per_session
FROM session_events_aggregated
GROUP BY DATE(event_hour)
ORDER BY session_date DESC;


-- Top active users by IP Addre 
SELECT ip, SUM(num_hits) AS total_hits, COUNT(*) AS total_sessions
FROM session_events_aggregated
GROUP BY ip
ORDER BY total_hits DESC
LIMIT 20;

-- Sessions distribution analysis
SELECT 
    CASE 
        WHEN num_hits <= 1 THEN '1'
        WHEN num_hits <= 5 THEN '2-5'
        WHEN num_hits <= 10 THEN '6-10'
        ELSE '10+' 
    END AS session_size,
    COUNT(*) AS session_count
FROM session_events_aggregated
GROUP BY session_size
ORDER BY session_size;

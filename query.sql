WITH start_users AS (
  SELECT 
    level, 
    COUNT(DISTINCT user_pseudo_id) as start_user_general,
    COUNT(user_pseudo_id) as start_event_general,

    COUNT(DISTINCT CASE WHEN revive = 0  THEN user_pseudo_id END) as start_user_not_revive,
    COUNT(CASE WHEN revive = 0  THEN user_pseudo_id END) as start_event_not_revive,
    COUNT(DISTINCT CASE WHEN booster_use = 0  THEN user_pseudo_id END) as start_user_not_booster,
    COUNT(CASE WHEN booster_use = 0  THEN user_pseudo_id END) as start_event_not_booster,
    COUNT(DISTINCT CASE WHEN booster_use = 0  and revive = 0 THEN user_pseudo_id END) as start_user_not_resource,
    COUNT(CASE WHEN booster_use = 0  and revive = 0 THEN user_pseudo_id END) as start_event_not_resource,

    COUNT(DISTINCT CASE WHEN revive > 0 and booster_use = 0 THEN user_pseudo_id END) as start_user_revive,
    COUNT(CASE WHEN revive > 0 and booster_use = 0 THEN user_pseudo_id END) as start_event_revive,
    COUNT(DISTINCT CASE WHEN booster_use > 0 and revive = 0 THEN user_pseudo_id END) as start_user_booster,
    COUNT(CASE WHEN booster_use > 0 and revive = 0 THEN user_pseudo_id END) as start_event_booster,
    COUNT(DISTINCT CASE WHEN revive > 0 and booster_use > 0 THEN user_pseudo_id END) as start_user_revive_booster,
    COUNT(CASE WHEN revive > 0 and booster_use > 0 THEN user_pseudo_id END) as start_event_revive_booster,

    COUNT(DISTINCT CASE WHEN attempt_times = 1 THEN user_pseudo_id END) as start_user_att1,
    COUNT(CASE WHEN attempt_times = 1 THEN user_pseudo_id END) as start_event_att1,
    COUNT(DISTINCT CASE WHEN attempt_times = 2 THEN user_pseudo_id END) as start_user_att2,
    COUNT(CASE WHEN attempt_times = 2 THEN user_pseudo_id END) as start_event_att2,

    COUNT(DISTINCT CASE WHEN attempt_times = 1 and revive > 0 and booster_use = 0 THEN user_pseudo_id END) as start_user_att1_revive,
    COUNT(CASE WHEN attempt_times = 1 and revive > 0 and booster_use = 0 THEN user_pseudo_id END) as start_event_att1_revive,
    COUNT(DISTINCT CASE WHEN attempt_times = 2 and revive > 0 and booster_use = 0 THEN user_pseudo_id END) as start_user_att2_revive,
    COUNT(CASE WHEN attempt_times = 2 and revive > 0 and booster_use = 0 THEN user_pseudo_id END) as start_event_att2_revive,

    COUNT(DISTINCT CASE WHEN attempt_times = 1 and revive = 0 and booster_use > 0 THEN user_pseudo_id END) as start_user_att1_booster,
    COUNT(CASE WHEN attempt_times = 1 and revive = 0 and booster_use > 0 THEN user_pseudo_id END) as start_event_att1_booster,
    COUNT(DISTINCT CASE WHEN attempt_times = 2 and revive = 0 and booster_use > 0 THEN user_pseudo_id END) as start_user_att2_booster,
    COUNT(CASE WHEN attempt_times = 2 and revive = 0 and booster_use > 0 THEN user_pseudo_id END) as start_event_att2_booster,

    COUNT(DISTINCT CASE WHEN attempt_times = 1 and revive > 0 and booster_use > 0 THEN user_pseudo_id END) as start_user_att1_revive_booster,
    COUNT(CASE WHEN attempt_times = 1 and revive > 0 and booster_use > 0 THEN user_pseudo_id END) as start_event_att1_revive_booster,
    COUNT(DISTINCT CASE WHEN attempt_times = 2 and revive > 0 and booster_use > 0 THEN user_pseudo_id END) as start_user_att2_revive_booster,
    COUNT(CASE WHEN attempt_times = 2 and revive > 0 and booster_use > 0 THEN user_pseudo_id END) as start_event_att2_revive_booster,
  FROM `crazy-coffee-jam.dashboard_table.level_data`
  WHERE event_date >= '2025-06-17' 
    AND version = '1.0.18' 
    AND level <= 100
  GROUP BY level
),  
win_stats AS (
  SELECT
    level,
    COUNT(DISTINCT user_pseudo_id) as win_user,
    SUM(COALESCE(revive,0)) as num_revive,
    SUM(COALESCE(booster_use,0)) as num_booster,
    AVG(count_sec) as avg_time_play_win,
    STDDEV(count_sec) as std_time_play_win,
    MAX(count_sec) as max_time_play_win,
    MIN(count_sec) as min_time_play_win,

    COUNT(DISTINCT CASE WHEN revive = 0 THEN user_pseudo_id END) as num_user_not_revive,
    AVG(CASE WHEN revive = 0 THEN count_sec END) as avg_time_win_not_revive,
    STDDEV(CASE WHEN revive = 0 THEN count_sec END) AS std_time_win_not_revive,
    MAX(CASE WHEN revive = 0 THEN count_sec END) AS max_time_win_not_revive,
    MIN(CASE WHEN revive = 0 THEN count_sec END) AS min_time_win_not_revive,

    COUNT(DISTINCT CASE WHEN booster_use = 0 THEN user_pseudo_id END) as num_user_not_booster,
    AVG(CASE WHEN booster_use = 0 THEN count_sec END) as avg_time_win_not_booster,
    STDDEV(CASE WHEN booster_use = 0 THEN count_sec END) AS std_time_win_not_booster,
    MAX(CASE WHEN booster_use = 0 THEN count_sec END) AS max_time_win_not_booster,
    MIN(CASE WHEN booster_use = 0 THEN count_sec END) AS min_time_win_not_booster,

    COUNT(DISTINCT CASE WHEN booster_use = 0 and revive = 0 THEN user_pseudo_id END) as num_user_not_resource,
    AVG(CASE WHEN booster_use = 0 and revive = 0 THEN count_sec END) as avg_time_win_not_resource,
    STDDEV(CASE WHEN booster_use = 0 and revive = 0 THEN count_sec END) AS std_time_win_not_resource,
    MAX(CASE WHEN booster_use = 0 and revive = 0 THEN count_sec END) AS max_time_win_not_resource,
    MIN(CASE WHEN booster_use = 0 and revive = 0 THEN count_sec END) AS min_time_win_not_resource,


    COUNT(DISTINCT CASE WHEN revive > 0 and booster_use = 0 THEN user_pseudo_id END) as num_user_revive,
    AVG(CASE WHEN revive > 0 and booster_use = 0 THEN count_sec END) as avg_time_win_revive,
    STDDEV(CASE WHEN revive > 0 and booster_use = 0 THEN count_sec END) AS std_time_win_revive,
    MAX(CASE WHEN revive > 0 and booster_use = 0 THEN count_sec END) AS max_time_win_revive,
    MIN(CASE WHEN revive > 0 and booster_use = 0 THEN count_sec END) AS min_time_win_revive,

    COUNT(DISTINCT  CASE WHEN booster_use > 0 and revive = 0 THEN user_pseudo_id END) as num_user_booster,
    AVG(CASE WHEN booster_use > 0 and revive = 0 THEN count_sec END) as avg_time_win_booster,
    STDDEV(CASE WHEN booster_use > 0 and revive = 0 THEN count_sec END) AS std_time_win_booster,
    MAX(CASE WHEN booster_use > 0 and revive = 0 THEN count_sec END) AS max_time_win_booster,
    MIN(CASE WHEN booster_use > 0 and revive = 0 THEN count_sec END) AS min_time_win_booster,

    COUNT(DISTINCT CASE WHEN booster_use > 0 and revive > 0 THEN user_pseudo_id END) as num_user_booster_revive,
    AVG(CASE WHEN booster_use > 0 and revive > 0 THEN count_sec END) as avg_time_win_booster_revive,
    STDDEV(CASE WHEN booster_use > 0 and revive > 0 THEN count_sec END) AS std_time_win_booster_revive,
    MAX(CASE WHEN booster_use > 0 and revive > 0 THEN count_sec END) AS max_time_win_booster_revive,
    MIN(CASE WHEN booster_use > 0 and revive > 0 THEN count_sec END) AS min_time_win_booster_revive,


    COUNT(DISTINCT CASE WHEN win_at_attempt = 1 THEN user_pseudo_id END) as num_win_user_att1,
    AVG(CASE WHEN win_at_attempt = 1 THEN count_sec END) AS avg_time_play_att1,
    STDDEV(CASE WHEN win_at_attempt = 1 THEN count_sec END) AS std_time_play_att1,
    MAX(CASE WHEN win_at_attempt = 1 THEN count_sec END) AS max_time_play_att1,
    MIN(CASE WHEN win_at_attempt = 1 THEN count_sec END) AS min_time_play_att1,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 2 THEN user_pseudo_id END) as num_win_user_att2,
    AVG(CASE WHEN win_at_attempt = 2 THEN count_sec END) AS avg_time_play_att2,
    STDDEV(CASE WHEN win_at_attempt = 2 THEN count_sec END) AS std_time_play_att2,
    MAX(CASE WHEN win_at_attempt = 2 THEN count_sec END) AS max_time_play_att2,
    MIN(CASE WHEN win_at_attempt = 2 THEN count_sec END) AS min_time_play_att2,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use = 0 THEN user_pseudo_id END) as num_win_user_att1_revive,
    AVG(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use = 0 THEN count_sec END) AS avg_time_play_att1_revive,
    STDDEV(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use = 0 THEN count_sec END) AS std_time_play_att1_revive,
    MAX(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use = 0 THEN count_sec END) AS max_time_play_att1_revive,
    MIN(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use = 0 THEN count_sec END) AS min_time_play_att1_revive,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use = 0 THEN user_pseudo_id END) as num_win_user_att2_revive,
    AVG(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use = 0 THEN count_sec END) AS avg_time_play_att2_revive,
    STDDEV(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use = 0 THEN count_sec END) AS std_time_play_att2_revive,
    MAX(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use = 0 THEN count_sec END) AS max_time_play_att2_revive,
    MIN(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use = 0 THEN count_sec END) AS min_time_play_att2_revive,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 1 and revive = 0 and booster_use > 0 THEN user_pseudo_id END) as num_win_user_att1_booster,
    AVG(CASE WHEN win_at_attempt = 1 and revive = 0 and booster_use > 0 THEN count_sec END) AS avg_time_play_att1_booster,
    STDDEV(CASE WHEN win_at_attempt = 1 and revive = 0 and booster_use > 0 THEN count_sec END) AS std_time_play_att1_booster,
    MAX(CASE WHEN win_at_attempt = 1 and revive = 0 and booster_use > 0 THEN count_sec END) AS max_time_play_att1_booster,
    MIN(CASE WHEN win_at_attempt = 1 and revive = 0 and booster_use > 0 THEN count_sec END) AS min_time_play_att1_booster,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 2 and revive = 0 and booster_use > 0 THEN user_pseudo_id END) as num_win_user_att2_booster,
    AVG(CASE WHEN win_at_attempt = 2 and revive = 0 and booster_use > 0 THEN count_sec END) AS avg_time_play_att2_booster,
    STDDEV(CASE WHEN win_at_attempt = 2 and revive = 0 and booster_use > 0 THEN count_sec END) AS std_time_play_att2_booster,
    MAX(CASE WHEN win_at_attempt = 2 and revive = 0 and booster_use > 0 THEN count_sec END) AS max_time_play_att2_booster,
    MIN(CASE WHEN win_at_attempt = 2 and revive = 0 and booster_use > 0 THEN count_sec END) AS min_time_play_att2_booster,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use > 0 THEN user_pseudo_id END) as num_win_user_att1_revive_booster,
    AVG(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use > 0 THEN count_sec END) AS avg_time_play_att1_revive_booster,
    STDDEV(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use > 0 THEN count_sec END) AS std_time_play_att1_revive_booster,
    MAX(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use > 0 THEN count_sec END) AS max_time_play_att1_revive_booster,
    MIN(CASE WHEN win_at_attempt = 1 and revive > 0 and booster_use > 0 THEN count_sec END) AS min_time_play_att1_revive_booster,

    COUNT(DISTINCT CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use > 0 THEN user_pseudo_id END) as num_win_user_att2_revive_booster,
    AVG(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use > 0 THEN count_sec END) AS avg_time_play_att2_revive_booster,
    STDDEV(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use > 0 THEN count_sec END) AS std_time_play_att2_revive_booster,
    MAX(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use > 0 THEN count_sec END) AS max_time_play_att2_revive_booster,
    MIN(CASE WHEN win_at_attempt = 2 and revive > 0 and booster_use > 0 THEN count_sec END) AS min_time_play_att2_revive_booster
  
  FROM `crazy-coffee-jam.dashboard_table.win_level_time_attemp`
  WHERE event_date >= '2025-06-17'
    AND version = '1.0.18'
    AND level <= 100
  GROUP BY level
)

SELECT
  s.level,
  s.start_user_general,
  s.start_event_general,

  s.start_user_not_revive,
  s.start_event_not_revive,

  s.start_user_not_booster,
  s.start_event_not_booster,

  s.start_user_not_resource,
  s.start_event_not_resource,

  s.start_user_revive,
  s.start_event_revive,

  s.start_user_booster,
  s.start_event_booster,

  s.start_user_revive_booster,
  s.start_event_revive_booster,

  s.start_user_att1,
  s.start_event_att1,

  s.start_user_att2,
  s.start_event_att2,

  s.start_user_att1_revive,
  s.start_event_att1_revive,

  s.start_user_att2_revive,
  s.start_event_att2_revive,

  s.start_user_att1_booster,
  s.start_event_att1_booster,

  s.start_user_att2_booster,
  s.start_event_att2_booster,

  s.start_user_att1_revive_booster,
  s.start_event_att1_revive_booster,
  
  s.start_user_att2_revive_booster,
  s.start_event_att2_revive_booster,
  w.win_user,
  COALESCE(w.num_booster,0) as num_booster,
  COALESCE(w.num_revive,0) as num_revive,
  COALESCE(w.avg_time_play_win,0) as avg_time_play_win,
  COALESCE(w.std_time_play_win,0) as std_time_play_win,
  COALESCE(w.max_time_play_win,0) as max_time_play_win,
  COALESCE(w.min_time_play_win,0) as min_time_play_win,

  COALESCE(w.num_user_not_revive,0) as num_user_not_revive,
  COALESCE(w.avg_time_win_not_revive,0) as avg_time_win_not_revive,
  COALESCE(w.std_time_win_not_revive,0) as std_time_win_not_revive,
  COALESCE(w.max_time_win_not_revive,0) as max_time_win_not_revive,
  COALESCE(w.min_time_win_not_revive,0) as min_time_win_not_revive,

  COALESCE(w.num_user_not_booster,0) as num_user_not_booster,
  COALESCE(w.avg_time_win_not_booster,0) as avg_time_win_not_booster,
  COALESCE(w.std_time_win_not_booster,0) as std_time_win_not_booster,
  COALESCE(w.max_time_win_not_booster,0) as max_time_win_not_booster,
  COALESCE(w.min_time_win_not_booster,0) as min_time_win_not_booster,

  COALESCE(w.num_user_not_resource,0) as num_user_not_resource,
  COALESCE(w.avg_time_win_not_resource,0) as avg_time_win_not_resource,
  COALESCE(w.std_time_win_not_resource,0) as std_time_win_not_resource,
  COALESCE(w.max_time_win_not_resource,0) as max_time_win_not_resource,
  COALESCE(w.min_time_win_not_resource,0) as min_time_win_not_resource,

  COALESCE(w.num_user_revive,0) as num_user_revive,
  COALESCE(w.avg_time_win_revive,0) as avg_time_win_revive,
  COALESCE(w.std_time_win_revive,0) as std_time_win_revive,
  COALESCE(w.max_time_win_revive,0) as max_time_win_revive,
  COALESCE(w.min_time_win_revive,0) as min_time_win_revive,

  COALESCE(w.num_user_booster,0) as num_user_booster,
  COALESCE(w.avg_time_win_booster,0) as avg_time_win_booster,
  COALESCE(w.std_time_win_booster,0) as std_time_win_booster,
  COALESCE(w.max_time_win_booster,0) as max_time_win_booster,
  COALESCE(w.min_time_win_booster,0) as min_time_win_booster,

  COALESCE(w.num_user_booster_revive,0) as num_user_booster_revive,
  COALESCE(w.avg_time_win_booster_revive,0) as avg_time_win_booster_revive,
  COALESCE(w.std_time_win_booster_revive,0) as std_time_win_booster_revive,
  COALESCE(w.max_time_win_booster_revive,0) as max_time_win_booster_revive,
  COALESCE(w.min_time_win_booster_revive,0) as min_time_win_booster_revive,

  COALESCE(w.num_win_user_att1,0) as num_win_user_att1,
  COALESCE(w.avg_time_play_att1,0) as avg_time_play_att1,
  COALESCE(w.std_time_play_att1,0) as std_time_play_att1,
  COALESCE(w.max_time_play_att1,0) as max_time_play_att1,
  COALESCE(w.min_time_play_att1,0) as min_time_play_att1,

  COALESCE(w.num_win_user_att2,0) as num_win_user_att2,
  COALESCE(w.avg_time_play_att2,0) as avg_time_play_att2,
  COALESCE(w.std_time_play_att2,0) as std_time_play_att2,
  COALESCE(w.max_time_play_att2,0) as max_time_play_att2,
  COALESCE(w.min_time_play_att2,0) as min_time_play_att2,

  COALESCE(w.num_win_user_att1_revive,0) as num_win_user_att1_revive,
  COALESCE(w.avg_time_play_att1_revive,0) as avg_time_play_att1_revive,
  COALESCE(w.std_time_play_att1_revive,0) as std_time_play_att1_revive,
  COALESCE(w.max_time_play_att1_revive,0) as max_time_play_att1_revive,
  COALESCE(w.min_time_play_att1_revive,0) as min_time_play_att1_revive,

  COALESCE(w.num_win_user_att2_revive,0) as num_win_user_att2_revive,
  COALESCE(w.avg_time_play_att2_revive,0) as avg_time_play_att2_revive,
  COALESCE(w.std_time_play_att2_revive,0) as std_time_play_att2_revive,
  COALESCE(w.max_time_play_att2_revive,0) as max_time_play_att2_revive,
  COALESCE(w.min_time_play_att2_revive,0) as min_time_play_att2_revive,

  COALESCE(w.num_win_user_att1_booster,0) as num_win_user_att1_booster,
  COALESCE(w.avg_time_play_att1_booster,0) as avg_time_play_att1_booster,
  COALESCE(w.std_time_play_att1_booster,0) as std_time_play_att1_booster,
  COALESCE(w.max_time_play_att1_booster,0) as max_time_play_att1_booster,
  COALESCE(w.min_time_play_att1_booster,0) as min_time_play_att1_booster,

  COALESCE(w.num_win_user_att2_booster,0) as num_win_user_att2_booster,
  COALESCE(w.avg_time_play_att2_booster,0) as avg_time_play_att2_booster,
  COALESCE(w.std_time_play_att2_booster,0) as std_time_play_att2_booster,
  COALESCE(w.max_time_play_att2_booster,0) as max_time_play_att2_booster,
  COALESCE(w.min_time_play_att2_booster,0) as min_time_play_att2_booster, 

  COALESCE(w.num_win_user_att1_revive_booster,0) as num_win_user_att1_revive_booster,
  COALESCE(w.avg_time_play_att1_revive_booster,0) as avg_time_play_att1_revive_booster,
  COALESCE(w.std_time_play_att1_revive_booster,0) as std_time_play_att1_revive_booster,
  COALESCE(w.max_time_play_att1_revive_booster,0) as max_time_play_att1_revive_booster,
  COALESCE(w.min_time_play_att1_revive_booster,0) as min_time_play_att1_revive_booster,

  COALESCE(w.num_win_user_att2_revive_booster,0) as num_win_user_att2_revive_booster,
  COALESCE(w.avg_time_play_att2_revive_booster,0) as avg_time_play_att2_revive_booster,
  COALESCE(w.std_time_play_att2_revive_booster,0) as std_time_play_att2_revive_booster,
  COALESCE(w.max_time_play_att2_revive_booster,0) as max_time_play_att2_revive_booster,
  COALESCE(w.min_time_play_att2_revive_booster,0) as min_time_play_att2_revive_booster
FROM start_users s
LEFT JOIN win_stats w ON s.level = w.level
ORDER BY s.level;

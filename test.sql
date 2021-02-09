SELECT a.user_id
    , a.user_position
    -- , a.RNG
    , Min(date_position) AS start_date_position
    , Max(date_position) AS end_date_position
FROM (
    SELECT a.user_id
        , user_position
        , date_position
        , Sum(
            IS_CONT
        ) Over (PARTITION BY 1 ORDER BY user_id, user_position, date_position ASC ROWS Unbounded Preceding ) AS RNG
    FROM (
        SELECT user_id
             , user_position
             , date_position
            , Lag (date_position) Over (PARTITION BY user_id, user_position ORDER BY date_position) AS lag_date_position
            , CASE WHEN Coalesce(date_position - lag_date_position, 0) = 1 THEN 0 ELSE 1 END AS IS_CONT
        FROM ANP.T_TEST_DEL
    ) a
) a
GROUP BY a.user_id
    , a.user_position
    , a.RNG
ORDER BY  a.user_id
    , start_date_position
    , a.user_position

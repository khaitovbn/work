

WITH sky_01
AS (
    SELECT s.user_id
    	 , s.SUM_SESSION_BEGIN
    	 , s.data_time_min
    	 , s.data_time_max + interval '1 hour' AS data_time_max
    	-- , CASE WHEN s.CNT_PAGE > 1 THEN s.data_time_max ELSE s.data_time_max + interval '1 hour' END AS data_time_max
    FROM (
            SELECT s.USER_ID
            	 , s.SUM_SESSION_BEGIN
            	 , MIN (s.HAPPENED_AT) AS data_time_min
            	 , MAX (s.HAPPENED_AT) AS data_time_max
            	 , COUNT (DISTINCT s.PAGE) AS CNT_PAGE
            FROM (
                    SELECT s.USER_ID
                        , s.PAGE
                        , s.HAPPENED_AT
                        , s.LAG_DATE
                        , s.SESSION_BEGIN
                        , SUM(s.SESSION_BEGIN) OVER(PARTITION BY s.USER_ID ORDER BY s.HAPPENED_AT rows between unbounded preceding and current row) AS SUM_SESSION_BEGIN
                    FROM (
                                SELECT s.USER_ID
                                        , s.PAGE
                            			, s.HAPPENED_AT
                            			, LAG (s.HAPPENED_AT) OVER (PARTITION BY s.USER_ID ORDER BY s.HAPPENED_AT) AS LAG_DATE
                            			, CASE WHEN COALESCE(extract('epoch' FROM HAPPENED_AT - LAG(HAPPENED_AT) OVER(PARTITION BY s.USER_ID ORDER BY HAPPENED_AT)), 0) < 3600
                                               THEN 0 ELSE 1 END SESSION_BEGIN
                            	FROM test.vimbox_pages AS s
                    	) AS s
                    GROUP BY s.USER_ID
                            , s.PAGE
                            , s.HAPPENED_AT
                            , s.LAG_DATE
                            , s.SESSION_BEGIN
                            ) AS s
            GROUP BY s.USER_ID
            	 , s.SUM_SESSION_BEGIN
    	) AS s
    ORDER BY USER_ID
),

sky_02 AS (
    SELECT s.user_id
    	 , s.PAGE
    	 , s.HAPPENED_AT
    	 , a.SUM_SESSION_BEGIN
    	 , a.data_time_min
    	 , a.data_time_max
    	 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
    			WHEN s.PAGE = 'rooms.view.step.content' THEN 2
    			WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
    FROM test.vimbox_pages AS s 
    LEFT JOIN sky_01 AS a
    	ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
    	AND s.user_id = a.user_id
),

sky_user_1 AS ( SELECT DISTINCT s.user_id
            				, s.page_id
            				, s.SUM_SESSION_BEGIN
            		FROM sky_02 AS s
            		WHERE page_id = 1
),	
sky_user_2 AS (SELECT DISTINCT s.user_id
            					, s.page_id
            					, s.SUM_SESSION_BEGIN
            			FROM sky_02 AS s
            			WHERE page_id = 2
), 
sky_user_3 AS (SELECT DISTINCT s.user_id
            					, s.page_id
            					, s.SUM_SESSION_BEGIN
            			FROM sky_02 AS s
            			WHERE page_id = 3
),
sky_user_id AS (
    SELECT a.user_id
    FROM sky_user_1 AS a 
    INNER JOIN sky_user_2 AS b
        ON a.user_id = b.user_id
        AND a.SUM_SESSION_BEGIN = b.SUM_SESSION_BEGIN
    INNER JOIN sky_user_3 AS c
    	ON a.user_id = c.user_id
    	AND b.user_id = c.user_id
    	AND a.SUM_SESSION_BEGIN = c.SUM_SESSION_BEGIN
    	AND b.SUM_SESSION_BEGIN = c.SUM_SESSION_BEGIN
)


SELECT s.user_id
     , s.data_time_min
     , s.data_time_max
     , SUM (s.LAG_PAGE) AS CNT_LAG_PAGE
FROM (
        SELECT s.user_id
             , s.PAGE
             , s.page_id
             , CASE WHEN LAG (s.page_id) OVER (PARTITION BY s.user_id, s.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) > s.page_id THEN 1 ELSE 0 END AS LAG_PAGE
             , s.HAPPENED_AT
             , s.SUM_SESSION_BEGIN
             , s.data_time_min
             , s.data_time_max
        FROM (
                SELECT s.user_id
                     , s.PAGE
                     , s.HAPPENED_AT
                     , s.SUM_SESSION_BEGIN
                     , s.data_time_min
                     , s.data_time_max
                     , s.page_id
                FROM sky_02 AS s
                INNER JOIN sky_user_id AS a
                    ON s.user_id = a.user_id
                WHERE s.page_id IN (1,2,3)
            ) AS s
    ) AS s
GROUP BY s.user_id
     , s.data_time_min
     , s.data_time_max
HAVING SUM (s.LAG_STR_ID) = 0





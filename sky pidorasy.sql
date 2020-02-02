



WITH sky_02 AS (
SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id
)



----------------------------------------------------------------------------------------------------------------------------------------------------
WITH
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
)

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





























SELECT DISTINCT PAGE FROM test.vimbox_pages


WITH sky_01
AS (
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
ORDER BY USER_ID
)
--SELECT * FROM sky_01
----------------------------------------------------------------------------------------------------------------------------------------------------

SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id

 
----------------------------------------------------------------------------------------------------------------------------------------------------
WITH
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
)

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


----------------------------------------------------------------------------------------------------------------------------------------------------




WITH
sky_user_1 AS ( SELECT DISTINCT s.user_id
            				, s.page_id
            				, s.SUM_SESSION_BEGIN
            		FROM (
            		SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id

            		) AS s
            		WHERE page_id = 1
),	
sky_user_2 AS (SELECT DISTINCT s.user_id
            					, s.page_id
            					, s.SUM_SESSION_BEGIN
            			FROM (
            			SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id

            			) AS s
            			WHERE page_id = 2
), 
sky_user_3 AS (SELECT DISTINCT s.user_id
            					, s.page_id
            					, s.SUM_SESSION_BEGIN
            			FROM (
            			SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id
            			) AS s
            			WHERE page_id = 3
)

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

            
            
----------------------------------------------------------------------------------------------------------------------------------------------------            



SELECT a.user_id
FROM ( SELECT DISTINCT s.user_id
            				, s.page_id
            				, s.SUM_SESSION_BEGIN
            		FROM (
            		SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id

            		) AS s
            		WHERE page_id = 1
) AS a
INNER JOIN (SELECT DISTINCT s.user_id
            					, s.page_id
            					, s.SUM_SESSION_BEGIN
            			FROM (
            			SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id

            			) AS s
            			WHERE page_id = 2
) AS b
    ON a.user_id = b.user_id
    AND a.SUM_SESSION_BEGIN = b.SUM_SESSION_BEGIN
INNER JOIN (SELECT DISTINCT s.user_id
            					, s.page_id
            					, s.SUM_SESSION_BEGIN
            			FROM (
            			SELECT s.user_id
		--	 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_SESSION_BEGIN ORDER BY s.HAPPENED_AT) AS RN
			 , s.PAGE
			 , s.HAPPENED_AT
			 , a.SUM_SESSION_BEGIN
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.PAGE = 'rooms.homework-showcase' THEN 1
					WHEN s.PAGE = 'rooms.view.step.content' THEN 2
					WHEN s.PAGE = 'rooms.lesson.rev.step.content' THEN 3 ELSE NULL END AS page_id
		FROM test.vimbox_pages AS s 
		LEFT JOIN --sky_01
		(
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
	 )	AS a
			ON s.HAPPENED_AT BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id
            			) AS s
            			WHERE page_id = 3
) AS c
	ON a.user_id = c.user_id
	AND b.user_id = c.user_id
	AND a.SUM_SESSION_BEGIN = c.SUM_SESSION_BEGIN
	AND b.SUM_SESSION_BEGIN = c.SUM_SESSION_BEGIN






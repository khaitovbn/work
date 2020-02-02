

SELECT s.user_id
	 , s.str
	 , s.RN
	 , s.data_time
	 , s.data_time_prev
	 , s.DATE_
	 , s.DATE_PREV
	 , CASE WHEN s.DATE_ = 1 THEN s.data_time
				ELSE s.a END AS aaa
FROM (
		SELECT s.user_id
			 , s.str
			 , s.data_time
			 , s.RN
			 , s.data_time_prev
			-- , s.data_time - s.data_time_prev
			 , DATEADD (hh, 1, s.data_time_prev) AS a
			 , CASE WHEN s.data_time < DATEADD (hh, 1, s.data_time_prev) THEN 0 ELSE 1 END AS DATE_
			 , LAG (CASE WHEN s.data_time < DATEADD (hh, 1, s.data_time_prev) THEN 0 ELSE 1 END) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS DATE_PREV
		FROM (
				SELECT s.user_id
					 , s.str
					 , s.data_time
					 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS RN
					 , LAG (s.DATA_TIME) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS data_time_prev
				FROM NEWBIE.dbo.T_SKY AS s
			) AS s
	) AS s
WHERE s.user_id = 3




SELECT s.user_id
		, s.str
		, s.data_time
		, s.RN
		, s.data_time_prev
	-- , s.data_time - s.data_time_prev
		, DATEADD (hh, 1, s.data_time_prev) AS a
		, CASE WHEN s.data_time < DATEADD (hh, 1, s.data_time_prev) THEN 0 ELSE 1 END AS DATE_
		, LAG (CASE WHEN s.data_time < DATEADD (hh, 1, s.data_time_prev) THEN 0 ELSE 1 END) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS DATE_PREV
FROM (
		SELECT s.user_id
				, s.str
				, s.data_time
				, ROW_NUMBER () OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS RN
				, LAG (s.DATA_TIME) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS data_time_prev
		FROM NEWBIE.dbo.T_SKY AS s
	) AS s


SELECT s.user_id
	 , s.SUM_NUMBERS
	 , s.SUM_GRP_START
	 , MIN (s.data_time) AS data_time_min
	 , MAX (s.data_time) AS data_time_max
FROM (
		SELECT s.user_id
			 , s.data_time
			 , s.str
			 , s.NUMBERS
			 , s.LAG_NUMBERS
			 , s.LAG_DATA
			 , s.GRP_START
			 , SUM (s.LAG_NUMBERS) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS SUM_NUMBERS
			 , SUM (s.GRP_START) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS SUM_GRP_START
		FROM (
				SELECT s.user_id
					 , s.data_time
					 , s.str
					 , s.NUMBERS
					 , CASE WHEN LAG (s.NUMBERS) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) > s.NUMBERS THEN 1 ELSE 0 END AS LAG_NUMBERS
					 , s.LAG_DATA
					 , s.GRP_START
				FROM (
						SELECT s.user_id
							 , s.data_time
							 , s.str
							 , CASE WHEN s.str = 'список ДЗ' THEN 1
									WHEN s.str = 'ДЗ' THEN 2
									WHEN s.str = 'Урок с преподавателем' THEN 3 ELSE NULL END AS NUMBERS
							 , LAG (s.DATA_TIME) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS LAG_DATA
							 , CASE WHEN COALESCE (CAST (s.DATA_TIME AS FLOAT) - CAST (LAG (s.DATA_TIME) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS FLOAT), 0) < 0.0416666666715173 
										THEN 0 ELSE 1 END AS GRP_START
						FROM NEWBIE.dbo.T_SKY AS s
					) AS s
			) AS s
	) AS s
GROUP BY s.user_id
	 , s.SUM_NUMBERS
	 , s.SUM_GRP_START


--	==============================================================================================================================

SELECT *
FROM NEWBIE.dbo.T_SKY AS s


--INSERT INTO NEWBIE.dbo.T_SKY
--VALUES (6, 'Урок с преподавателем', '2020-01-01 13:40:00.000');

--INSERT INTO NEWBIE.dbo.T_SKY
--VALUES (6, 'Любая страница', '2020-01-01 13:45:00.000');

--INSERT INTO NEWBIE.dbo.T_SKY
--VALUES (6, 'список ДЗ', '2020-01-01 13:50:00.000');

--INSERT INTO NEWBIE.dbo.T_SKY
--VALUES (6, 'ДЗ', '2020-01-01 13:58:00.000');


SELECT *
FROM NEWBIE.dbo.T_SKY_01

DROP TABLE NEWBIE.dbo.T_SKY_01;
SELECT s.user_id
	 , s.SUM_GRP_START
	 , s.data_time_min
	 , CASE WHEN s.CNT_STR > 1 THEN s.data_time_max ELSE DATEADD (hh, 1, s.data_time_max) END AS data_time_max
INTO NEWBIE.dbo.T_SKY_01
FROM (
		SELECT s.user_id
			 , s.SUM_GRP_START
			 , MIN (s.data_time) AS data_time_min
			 , MAX (s.data_time) AS data_time_max
			 , COUNT (DISTINCT s.str) AS CNT_STR
		FROM (
				SELECT s.user_id
					 , s.data_time
					 , s.str
					 , s.LAG_DATA
					 , s.GRP_START
					 , SUM (s.GRP_START) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS SUM_GRP_START
				FROM (
						SELECT s.user_id
								, s.data_time
								, s.str
								, LAG (s.DATA_TIME) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS LAG_DATA
								, CASE WHEN COALESCE (CAST (s.DATA_TIME AS FLOAT) - CAST (LAG (s.DATA_TIME) OVER (PARTITION BY s.USER_ID ORDER BY s.DATA_TIME) AS FLOAT), 0) < 0.0416666666715173 
										THEN 0 ELSE 1 END AS GRP_START
						FROM NEWBIE.dbo.T_SKY AS s
					) AS s
			) AS s
		GROUP BY s.user_id
			 , s.SUM_GRP_START
	--	ORDER BY USER_ID
	) AS s
ORDER BY USER_ID;


SELECT s.user_id
	 , s.SUM_GRP_START
	 , s.data_time_min
	 , s.data_time_max
FROM NEWBIE.dbo.T_SKY_01 AS s


DROP TABLE NEWBIE.dbo.T_SKY_02;
SELECT s.user_id
	 , s.RN
	 , s.str
	 , s.str_id
--	 , CASE WHEN LAG (s.str_id) OVER (PARTITION BY s.USER_ID, s.SUM_GRP_START ORDER BY s.DATA_TIME) > s.str_id THEN 1 ELSE 0 END AS PR_STR_ID
	 , s.data_time
	 , s.SUM_GRP_START
	 , s.data_time_min
	 , s.data_time_max
INTO NEWBIE.dbo.T_SKY_02
FROM (
		SELECT s.user_id
			 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, a.SUM_GRP_START ORDER BY s.data_time) AS RN
			 , s.str
			 , s.data_time
			 , a.SUM_GRP_START
			 , a.data_time_min
			 , a.data_time_max
			 , CASE WHEN s.str = 'список ДЗ' THEN 1
					WHEN s.str = 'ДЗ' THEN 2
					WHEN s.str = 'Урок с преподавателем' THEN 3 ELSE NULL END AS str_id
		FROM NEWBIE.dbo.T_SKY AS s 
		LEFT JOIN NEWBIE.dbo.T_SKY_01 AS a
			ON s.data_time BETWEEN a.data_time_min AND a.data_time_max
			AND s.user_id = a.user_id
	) AS s
ORDER BY s.user_id, s.RN;


SELECT *
FROM NEWBIE.dbo.T_SKY_02



DROP TABLE NEWBIE.dbo.T_SKY_USER;
SELECT a.user_id
INTO NEWBIE.dbo.T_SKY_USER
FROM (
		SELECT DISTINCT s.user_id
				, s.str_id
				, s.SUM_GRP_START
		FROM NEWBIE.dbo.T_SKY_02 AS s
		WHERE str_id = 1
	) AS a
INNER JOIN (
			SELECT DISTINCT s.user_id
					, s.str_id
					, s.SUM_GRP_START
			FROM NEWBIE.dbo.T_SKY_02 AS s
			WHERE str_id = 2
		) AS b
	ON a.user_id = b.user_id
	AND a.SUM_GRP_START = b.SUM_GRP_START
INNER JOIN (
			SELECT DISTINCT s.user_id
					, s.str_id
					, s.SUM_GRP_START
			FROM NEWBIE.dbo.T_SKY_02 AS s
			WHERE str_id = 3
	) AS c
	ON a.user_id = c.user_id
	AND b.user_id = c.user_id
	AND a.SUM_GRP_START = c.SUM_GRP_START
	AND b.SUM_GRP_START = c.SUM_GRP_START;


SELECT *
FROM NEWBIE.dbo.T_SKY_USER

SELECT s.user_id
	 , s.data_time_min
	 , s.data_time_max
	 , SUM (s.LAG_STR_ID) AS CNT_LAG_STR
FROM (
		SELECT s.user_id
			 , s.str
			 , s.str_id
			 , CASE WHEN LAG (s.str_id) OVER (PARTITION BY s.USER_ID, s.SUM_GRP_START ORDER BY s.data_time) > s.str_id THEN 1 ELSE 0 END AS LAG_STR_ID
			 , s.SUM_GRP_START
			 , s.data_time
			 , s.data_time_min
			 , s.data_time_max
		FROM (
				SELECT s.user_id
					 , ROW_NUMBER () OVER (PARTITION BY s.USER_ID, s.SUM_GRP_START ORDER BY s.data_time) AS RN
					 , s.str
					 , s.str_id
					 , s.SUM_GRP_START
					 , s.data_time
					 , s.data_time_min
					 , s.data_time_max
				FROM NEWBIE.dbo.T_SKY_02 AS s
				INNER JOIN NEWBIE.dbo.T_SKY_USER AS a
					ON s.user_id = a.user_id
				WHERE s.str_id IN (1,2,3)
			) AS s
	) AS s
GROUP BY s.user_id
	 , s.data_time_min
	 , s.data_time_max
HAVING SUM (s.LAG_STR_ID) = 0


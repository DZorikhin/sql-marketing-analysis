-- create static table WITH permissions for product
drop table if exists [UsersTmp].[name].[perm_review]

SELECT DISTINCT  product_permissions.consumerId, product_permissions.resourceId,
product_permissions.[begin], product_permissions.[end], Product_permissions.rn
into [UsersTmp].[name].[perm_review]
FROM
(SELECT consumerId
		, ROW_NUMBER() OVER (PARTITION BY consumerId, resourceId ORDER BY offset DESC) rn
		, resourceId
		, [begin]
		, [end]
		, deleted
		, flags
 FROM [Permissions].[dbo].[ServiceInfos] WITH (nolock)
 WHERE resourceId IN ('23', '23.1', '23.2', '23.3', '23.4', '23.5', '23.6',
	 '23.7', '34.2.1')
) product_permissions
	WHERE YEAR([end]) >= 2015
	AND deleted IS NULL
	AND flags!=2
	AND rn < 15

-- create temporary table based on join on AbonId FROM ProductAccounts
drop table if exists #permissions_users_Product
SELECT DISTINCT eaa.consumerId, ea.UserId as PortalUserIdProduct, [begin],
[end], rn
into #permissions_users_Product
FROM [UsersTmp].[name].[perm_review] AS eaa  WITH (NOLOCK)
LEFT JOIN [Requisites].[dbo].[ProductAccounts] AS ea  WITH (NOLOCK)
  ON eaa.ConsumerID  = ea.AbonId

-- create temporary table based on join on ProductId FROM ProductMain
drop table if exists #permissions_users_Product_Main
SELECT DISTINCT ext.consumerId, PortalUserIdMain,
em.UserId AS PortalUserIdProduct, [begin], [end], rn
into #permissions_users_Main_Product
FROM #permissions_users_Main AS ext WITH (NOLOCK)
LEFT JOIN [Requisites].[dbo].[ProductMain] AS em WITH (NOLOCK)
  ON ext.ConsumerID = em.ProductId

-- create temporary table WITHout NULL values
drop table if exists #perm_to_merge
SELECT DISTINCT consumerId, PortalUserIdMain, PortalUserIdProduct, [begin],
[end], rn
into #perm_to_merge
FROM #permissions_users_Main_Product WITH (nolock)
WHERE PortalUserIdMain IS NOT NULL OR PortalUserIdProduct IS NOT NULL

-- number of permissions based on Id for portal users
SELECT COUNT(DISTINCT cp.consumerId) AS consumerIdcount, gp.resourceId
FROM (SELECT DISTINCT consumerId FROM #perm_to_merge WITH (nolock)) AS cp
JOIN [UsersTmp].[name].[perm_review] AS gp WITH (nolock)
	ON cp.consumerId = gp.consumerId
GROUP BY gp.resourceId

-- create temporary table WITH portal Id users WITH union clause for convenient
-- merge WITH Clickhouse data
drop table if exists #t
SELECT DISTINCT PortalUserIdMain AS PortalUserIdPerm, [begin], [end], rn
into #t
FROM
(
SELECT DISTINCT PortalUserIdMain, [begin], [end], rn
FROM #perm_to_merge
WHERE PortalUserIdMain IS NOT NULL
union
SELECT DISTINCT PortalUserIdProduct, [begin], [end], rn
FROM #perm_to_merge
WHERE PortalUserIdProduct IS NOT NULL
) m

-- data from Clickhouse for portal users
drop table if exists #Product_2018
CREATE TABLE #Product_2018 (PortalUserId varchar(36), ClientIp varchar(36), FirstDayEntry date, LastDayEntry date, UniqueDaysEntry int)

INSERT #Product_2018
EXEC [Metrics].[dbo].[CH_query]
'SELECT
	PortalUserId,
	ClientIp,
	min(EventDate) AS FirstDayEntry,
	max(EventDate) AS LastDayEntry,
	uniqExact(EventDate) AS UniqueDaysEntry
FROM
	metrics.tracker_log

WHERE EventDate BETWEEN ''2018-01-01'' AND ''2018-12-31''
	AND SiteId = 17
	AND IsStaff != 1
GROUP BY PortalUserId, ClientIp
HAVING PortalUserId != ''00000000-0000-0000-0000-000000000000'''

CREATE INDEX ix_#perm_to_merge_PortalUserIdMain ON #perm_to_merge (PortalUserIdMain);
CREATE INDEX ix_#perm_to_merge_PortalUserIdProduct ON #perm_to_merge (PortalUserIdProduct);
CREATE INDEX ix_#Product_2018_PortalUserId ON #Product_2018 (PortalUserId);

-- SEGMENT 1. Entered Product WITHout permissions in 2018 (Demo_users)
-- unique portal users number in 2018 for Product demo is equal to XXXX
-- unique IP addresses number in 2018 for Product demo is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry, COUNT(DISTINCT ClientIp) AS IPUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE PortalUserIdPerm IS NULL

-- unique portal users number in 2018 for Product demo WITH scope/Product/inited requisite is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE PortalUserIdPerm IS NULL
AND PortalUserId  IN (SELECT DISTINCT Id FROM [Requisites].[dbo].[UserRequisites] WITH (nolock)
WHERE [scope/Product/inited] IS NOT NULL)

-- XXXX unique IP addresses which have been used to register more than 1 portal user
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry, ClientIp AS IPUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE PortalUserIdPerm IS NULL
GROUP BY ClientIp
HAVING COUNT(DISTINCT PortalUserId) > 1

-- number of unique portal users who had several registrations FROM single IP is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId)
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE
	PortalUserIdPerm IS NULL
	and ClientIp in (
		SELECT ClientIp
		FROM #Product_2018 AS ch WITH (nolock)
		LEFT JOIN #t AS t WITH (nolock)
			ON ch.PortalUserId = t.PortalUserIdPerm
		WHERE PortalUserIdPerm IS NULL
		GROUP BY ClientIp
		HAVING COUNT(DISTINCT PortalUserId) > 1
		)

-- XXXX PortalUsersId - XXXX PortalUsersId = XXXX users ; XXXX PortalUsersId is equal to XXXX UserIp
-- Total number of portal users is XXXX + XXXX = XXXX

-- number of portal users per month based on FirstEntryDate
SELECT MONTH(FirstDayEntry) AS [Month], COUNT(distinct PortalUserId) AS PortalUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE PortalUserIdPerm IS NULL
GROUP BY MONTH(FirstDayEntry)
ORDER BY MONTH(FirstDayEntry)

-- number of entries on unique days
SELECT COUNT(DISTINCT PortalUserId) AS CountEntry, UniqueDaysEntry
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE PortalUserIdPerm IS NULL
GROUP BY UniqueDaysEntry
ORDER BY UniqueDaysEntry


-- SEGMENT 2. Entered after permission end (Former_users)
-- unique portal users number in 2018 for Product is equal to XXXX
-- unique IP addresses number in 2018 for Product is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry, COUNT(DISTINCT ClientIp) AS IPUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE t.rn = 1 AND ch.LastDayEntry > t.[end]


-- unique portal users number in 2018 for Product demo WITH scope/Product/inited requisite is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE t.rn = 1 AND ch.LastDayEntry > t.[end]
AND PortalUserId  in (SELECT DISTINCT Id FROM [Requisites].[dbo].[UserRequisites] WITH (nolock)
WHERE [scope/Product/inited] IS NOT NULL)

-- XXXX unique IP addresses which have been used to register more than 1 portal user
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry, ClientIp AS IPUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE t.rn = 1 AND ch.LastDayEntry > t.[end]
GROUP BY ClientIp
HAVING COUNT(DISTINCT PortalUserId) > 1

-- number of unique portal users who had several registrations FROM single IP is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId)
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE
	t.rn = 1 AND ch.LastDayEntry > t.[end]
	and ClientIp in (
		SELECT ClientIp
		FROM #Product_2018 AS ch WITH (nolock)
		LEFT JOIN #t AS t WITH (nolock)
			ON ch.PortalUserId = t.PortalUserIdPerm
		WHERE t.rn = 1 AND ch.LastDayEntry > t.[end]
		GROUP BY ClientIp
		HAVING COUNT(DISTINCT PortalUserId) > 1
		)

-- XXXX Portal Users IDs - XXXX Portal Users IDs = XXXX users ; XXXX Portal Users IDs is equal to XXXX User IPs
-- Total number of unique users is XXXX + XXXX = XXXX

-- number of portal users per month based on FirstEntryDate
SELECT MONTH(FirstDayEntry) AS [Month], COUNT(distinct PortalUserId) AS PortalUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE t.rn = 1 AND ch.LastDayEntry > t.[end]
GROUP BY MONTH(FirstDayEntry)
ORDER BY MONTH(FirstDayEntry)

-- number of portal users which have entered Product on unique days
SELECT COUNT(DISTINCT PortalUserId) AS CountEntry, UniqueDaysEntry
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #t AS t WITH (nolock)
	ON ch.PortalUserId = t.PortalUserIdPerm
WHERE t.rn = 1 AND ch.LastDayEntry > t.[end]
GROUP BY UniqueDaysEntry
ORDER BY UniqueDaysEntry


-- SEGMENT 3. Entered before permission start (Selfbuy_users)
drop table if exists #bk_crossed
go

SELECT
 v_begin.[begin]
, min(v_end.[end]) enddate
, v_begin.PortalUserIdPerm

into #bk_crossed
  FROM
       ( -- found all starts of ranges:
          SELECT [begin], PortalUserIdPerm
            FROM #t s1
           WHERE NOT EXISTS (
                             SELECT NULL
                             FROM #t s2
                             WHERE s1.[begin] > s2.[begin]
                                  and s1.[begin] <= s2.[end]
          AND s1.PortalUserIdPerm = s2.PortalUserIdPerm

                            )
       ) v_begin
  JOIN
       ( -- found all endings of ranges:
          SELECT [end], PortalUserIdPerm
            FROM #t s1
           WHERE NOT EXISTS (
                               select null
                                 FROM #t s2
                                WHERE s2.[end] > s1.[end]
                                  and s2.[begin] <= s1.[end]
          AND s1.PortalUserIdPerm = s2.PortalUserIdPerm

                            )
       ) v_end
--
    ON v_begin.PortalUserIdPerm = v_end.PortalUserIdPerm

 AND v_begin.[begin] <= v_end.[end]
GROUP BY v_begin.PortalUserIdPerm, v_begin.[begin]
ORDER BY v_begin.PortalUserIdPerm

-- entered before permission start (Selfbuy_users)
-- unique portal users number in 2018 for Product is equal to XXXX
-- unique IP addresses number in 2018 for Product is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry, COUNT(DISTINCT ClientIp) AS IPUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #bk_crossed AS bkc WITH (nolock)
	ON ch.PortalUserId = bkc.PortalUserIdPerm
WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]

-- unique portal users number in 2018 for Product demo WITH scope/Product/inited requisite is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #bk_crossed AS bkc WITH (nolock)
	ON ch.PortalUserId = bkc.PortalUserIdPerm
WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]
and PortalUserId  in (SELECT DISTINCT Id FROM [Requisites].[dbo].[UserRequisites] WITH (nolock)
WHERE [scope/Product/inited] IS NOT NULL)

-- XXXX unique IP addresses which have been used to register more than 1 portal user
SELECT COUNT(DISTINCT PortalUserId) AS DemoEntry, ClientIp AS IPUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #bk_crossed AS bkc WITH (nolock)
	ON ch.PortalUserId = bkc.PortalUserIdPerm
WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]
GROUP BY ClientIp
HAVING COUNT(DISTINCT PortalUserId) > 1

-- number of unique portal users who had several registrations FROM single IP is equal to XXXX
SELECT COUNT(DISTINCT PortalUserId)
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #bk_crossed AS bkc WITH (nolock)
	ON ch.PortalUserId = bkc.PortalUserIdPerm
WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]
	and ClientIp in (
		SELECT ClientIp
		FROM #Product_2018 AS ch WITH (nolock)
		LEFT JOIN #bk_crossed AS bkc WITH (nolock)
			ON ch.PortalUserId = bkc.PortalUserIdPerm
		WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]
		GROUP BY ClientIp
		HAVING COUNT(DISTINCT PortalUserId) > 1
		)

-- XXXX PortalUsersId - XXXX PortalUsersId = XXXX users ; XXXX PortalUsersId is equal to XXXX UserIp
-- Total number of portal users is XXXX + XXXX = XXXX

-- number of portal users per month based on FirstEntryDate
SELECT MONTH(FirstDayEntry) AS [Month], COUNT(distinct PortalUserId) AS PortalUsers
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #bk_crossed AS bkc WITH (nolock)
			ON ch.PortalUserId = bkc.PortalUserIdPerm
		WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]
GROUP BY MONTH(FirstDayEntry)
ORDER BY MONTH(FirstDayEntry)

-- number of entries on unique days
SELECT COUNT(DISTINCT PortalUserId) AS CountEntry, UniqueDaysEntry
FROM #Product_2018 AS ch WITH (nolock)
LEFT JOIN #bk_crossed AS bkc WITH (nolock)
			ON ch.PortalUserId = bkc.PortalUserIdPerm
		WHERE [begin] > '2018-01-01' AND ch.FirstDayEntry < bkc.[begin]
GROUP BY UniqueDaysEntry
ORDER BY UniqueDaysEntry

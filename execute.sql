CREATE DATABASE Monitoring
go

USE Monitoring

BEGIN TRANSACTION

CREATE TABLE Metrics (
    [Collect Time] DATETIME NOT NULL,
    [Memory: Available MB] BIGINT,
    [Page life expectancy in seconds] BIGINT,
    [Checkpoint Pages/sec] BIGINT,
    [Page Writes/sec] BIGINT,
    [Page Reads/sec] BIGINT,
    [Batch Requests/sec] BIGINT,
    [Page Cache Size (MB)] BIGINT,
    [Granted Workspace Memory (MB)] BIGINT,
    [Maximum Workspace Memory (MB)] BIGINT,
    [Memory Grants Pending] BIGINT,
    [Buffer Cache Hit Ratio] BIGINT,
    [Full scans/sec] BIGINT,
    [Log File(s) Used Size (MB)] BIGINT,
    [Processes Blocked] BIGINT,
    [Lock Waits/sec] BIGINT,
    [User Connections] BIGINT
);

CREATE CLUSTERED INDEX IX_CollectTime ON Metrics ([Collect Time] DESC)

CREATE TABLE RunningQueries (
    [Collect Time] datetime NOT NULL,
    session_id int,
    login_name nvarchar(128),
    transaction_id bigint,
    DatabaseName nvarchar(128),
    [status] nvarchar(30),
    start_time datetime,
    total_elapsed_time bigint,
    cpu_time bigint,
    logical_reads bigint,
    writes bigint,
    row_count bigint,
    wait_type nvarchar(60),
    wait_time bigint,
    [command] nvarchar(32),
    query_hash bigint,
    query_plan_hash bigint,
    requested_memory_mb float,
    granted_memory_mb float,
    used_memory_mb float,
    SqlQuery nvarchar(max)
);

CREATE CLUSTERED INDEX IX_CollectTime ON RunningQueries([Collect Time] DESC);

CREATE procedure sp_insert_perf_metrics -- Процедура, которая собирает счетчики
AS
	INSERT Metrics ([Collect time], [Memory: Available MB], [Page life expectancy in seconds], [Checkpoint Pages/sec], [Page Writes/sec], [Page Reads/sec], [Batch Requests/sec], [Page Cache Size (MB)], [Granted Workspace Memory (MB)], [Buffer Cache Hit Ratio], [Memory Grants Pending], [Full scans/sec], [Buffer Cache Hit Ratio], [Log File(s) Used Size (MB)], [Processes Blocked], [Lock Waits/sec], [User Connections])
    SELECT
        Collect_time,
        (select available_physical_memory_kb / 1024 FROM sys.dm_os_sys_memory) AS [Memory: Available MB], -- доступная оперативная память в мб
        [Page life expectancy] AS [Page life expectancy in seconds], -- (cple) сколько страницы живут в буфферном пуле
        [Checkpoint Pages/sec], -- позволит определить, нагружены ли диски из-за маленького плуа/больших объемов считываемой информации или же из-за того, что на диск постоянно сливаются «грязные страницы» (много изменяемых данных)
        [Page Writes/sec], -- количество записей на диск
        [Page Reads/sec], -- количество чтений с диска
        [Batch Requests/sec], --  позволит определить, в момент просадки cple не было ли повышенного количества запросов
        CAST([Database pages] AS FLOAT) * 8 / 1024 AS [Page Cache Size (MB)],
        CAST([Granted Workspace Memory (KB)] AS FLOAT) / 1024 AS [Granted Workspace Memory (MB)], -- покажет, не отъедается ли часть буфферного пула под память для запросов (hash-операции, сортировка)
        CAST([Maximum Workspace Memory (KB)] AS FLOAT) / 1024 AS [Maximum Workspace Memory (MB)],
        [Memory Grants Pending], -- запросы ожидающие выделения памяти
        [Buffer Cache Hit Ratio],
        [Full scans/sec],
        CAST([Log File(s) Used Size (KB)] AS FLOAT) / 1024 AS [Log File(s) Used Size (MB)], -- общий размер журнала
        [Processes Blocked],
        [Lock Waits/sec],
        [User Connections],
        [SQL Compilations/sec],
        [SQL Re-Compilations/sec]
    FROM (
        SELECT
            getutcdate() AS Collect_time,
            RTRIM(counter_name) AS Counter,
            cntr_value AS Value
        FROM sys.dm_os_performance_counters
        WHERE counter_name IN (
            N'Checkpoint Pages/sec',
            N'Page Reads/sec',
            N'Page Writes/sec',
            N'Full scans/sec',
            N'Processes Blocked',
            N'User Connections',
            N'SQL Re-Compilations/sec',
            N'SQL Compilations/sec',
            N'Batch Requests/sec',
            N'Page life expectancy',
            N'Buffer Cache Hit Ratio',
            N'Granted Workspace Memory (KB)',
            N'Maximum Workspace Memory (KB)',
            N'Memory Grants Pending',
            N'SQL Compilations/sec',
            N'SQL Re-Compilations/sec'
        )
        OR (counter_name = N'Lock Waits/sec' AND instance_name = '_Total')
        OR (counter_name = N'Log File(s) Used Size (KB)' AND instance_name = '_Total')
        OR (counter_name = 'Page life expectancy' AND object_name LIKE '%Buffer Manager%')
        OR (counter_name = 'Database pages' AND object_name LIKE '%Buffer Manager%')
    ) AS SourceTable
    PIVOT
    (
        MAX(Value)
        FOR Counter IN (
            [Checkpoint Pages/sec],
            [Page Reads/sec],
            [Page Writes/sec],
            [Full scans/sec],
            [Processes Blocked],
            [Lock Waits/sec],
            [User Connections],
            [Batch Requests/sec],
            [Page life expectancy],
            [Buffer Cache Hit Ratio],
            [Granted Workspace Memory (KB)],
            [Log File(s) Used Size (KB)],
            [Maximum Workspace Memory (KB)],
            [Memory Grants Pending],
            [Database pages],
            [SQL Compilations/sec],
            [SQL Re-Compilations/sec]
        )
    ) AS PivotTable;

CREATE PROCEDURE sp_insert_runningQueries
AS
BEGIN
    IF EXISTS (
        SELECT
            1
        FROM sys.dm_os_performance_counters
        WHERE counter_name = 'Memory Grants Pending'
        AND cntr_value > 0

        UNION ALL

        SELECT
            1
        FROM (
            SELECT TOP 1
                (CAST([Page Cache Size (MB)] AS FLOAT) / lead([Page Cache Size (MB)]) over (order by [Collect time] desc)) AS [Page Cache Size Increase Rate]
            FROM [Metrics]
            ORDER BY [Collect time] DESC
        ) src
        WHERE [src].[Page Cache Size Increase Rate] < 0.9 -- урезание кэша
    )
    BEGIN
        INSERT INTO RunningQueries (
            [Collect Time],
            session_id,
            login_name,
            transaction_id,
            DatabaseName,
            [status],
            start_time,
            total_elapsed_time,
            cpu_time,
            logical_reads,
            writes,
            row_count,
            wait_type,
            wait_time,
            [command],
            query_hash,
            query_plan_hash,
            requested_memory_mb,
            granted_memory_mb,
            used_memory_mb,
            SqlQuery
        )
        SELECT
            getutcdate() AS [Collect time],
            r.session_id,
            s.login_name,
            r.transaction_id,
            DB_NAME(r.database_id) AS DatabaseName,
            r.[status],
            r.start_time,
            r.total_elapsed_time,
            r.cpu_time,
            r.logical_reads,
            r.writes,
            r.row_count,
            r.wait_type,
            r.wait_time,
            r.[command],
            r.query_hash,
            r.query_plan_hash,
            CAST(mg.requested_memory_kb as float) / 1024 as requested_memory_mb,
            CAST(mg.granted_memory_kb as float) / 1024 as granted_memory_mb,
            CAST(mg.used_memory_kb as float) / 1024 as used_memory_mb,
            t.text AS SqlQuery
        FROM sys.dm_exec_requests r
        LEFT JOIN sys.dm_exec_sessions s ON r.session_id = s.session_id
        LEFT JOIN sys.dm_exec_query_memory_grants mg ON r.sql_handle = mg.sql_handle
        CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) t
        WHERE r.session_id != @@SPID
    END
END

COMMIT
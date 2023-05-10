declare @total_elapsed_time bigint = (select sum(total_elapsed_time) from sys.dm_exec_query_stats)

SELECT top 10
    last_execution_time,
    DATEDIFF(hour, creation_time, getdate()) as lifetime_hours,
    execution_count,
    ST.text as query_text,
    SUBSTRING(ST.text, (QS.statement_start_offset/2) + 1, ((
        IIF(statement_end_offset = -1,
            DATALENGTH(ST.text),
            QS.statement_end_offset) - QS.statement_start_offset)/2) + 1
    ) AS statement_text,
    total_ideal_grant_kb / 1024 / (CAST(total_elapsed_time as float) / 1000000) AS [transfer_memory(mb)_per_second],
    CAST(total_elapsed_time as float) / 1000000 AS total_elapsed_time,
    CAST(total_elapsed_time as float) / @total_elapsed_time * 100 AS total_elapsed_time_procent,
    CONVERT(DECIMAL (10,2), max_grant_kb /1024.0) AS max_grant_mb,
    CONVERT(DECIMAL (10,2), min_grant_kb /1024.0) AS min_grant_mb,
    CONVERT(DECIMAL (10,2), (total_grant_kb / execution_count) /1024.0) AS avg_grant_mb,
    CONVERT(DECIMAL (10,2), max_used_grant_kb /1024.0) AS max_grant_used_mb,
    CONVERT(DECIMAL (10,2), min_used_grant_kb /1024.0) AS min_grant_used_mb,
    CONVERT(DECIMAL (10,2), (total_used_grant_kb/ execution_count)  /1024.0) AS avg_grant_used_mb,
    CONVERT(DECIMAL (10,2), (total_ideal_grant_kb/ execution_count)  /1024.0) AS avg_ideal_grant_mb,
    CONVERT(DECIMAL (10,2), (total_ideal_grant_kb/ 1024.0)) AS total_grant_for_all_executions_mb,
    CAST(last_elapsed_time as float) / 1000 AS last_elapsed_ms
FROM sys.dm_exec_query_stats QS
CROSS APPLY sys.dm_exec_sql_text(QS.sql_handle) as ST

--ORDER BY total_ideal_grant_kb DESC -- выделено больше всего памяти
--ORDER BY execution_count -- самый частый запрос 
--ORDER BY total_ideal_grant_kb * total_elapsed_time DESC -- удерживал больше всего памяти по времени
--ORDER BY CAST(total_elapsed_time as float) / @total_elapsed_time DESC -- отрабатывал дольше всех относительно общего времени
select
    [Collect time],
    (CAST([Batch Requests/sec] as float) - lead([Batch Requests/sec]) over (order by [Collect time] desc)) / datediff(ss, lead([Collect time]) over (order by [Collect time] desc), [Collect time]) AS [Пакетных запросов в секунду],
    (CAST([Checkpoint Pages/sec] as float) - lead([Checkpoint Pages/sec]) over (order by [Collect time] desc)) / datediff(ss, lead([Collect time]) over (order by [Collect time] desc), [Collect time]) AS [Сбросов страниц на диск в секунду],
    (CAST([Page Reads/sec] as float) - lead([Page Reads/sec]) over (order by [Collect time] desc)) / datediff(ss, lead([Collect time]) over (order by [Collect time] desc), [Collect time]) AS [Операций чтения в секунду],
    (CAST([Page Writes/sec] as float) - lead([Page Writes/sec]) over (order by [Collect time] desc)) / datediff(ss, lead([Collect time]) over (order by [Collect time] desc), [Collect time]) AS [Операций записи в секунду]
from Metrics
where [Collect time] BETWEEN $__timeFrom() AND $__timeTo()
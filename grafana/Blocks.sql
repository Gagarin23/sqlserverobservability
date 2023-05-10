select
    [Collect time],
    [Processes Blocked] AS [Транзакции ожидающие освобождения ресурсов],
    (CAST([Lock Waits/sec] as float) - lead([Lock Waits/sec]) over (order by [Collect time] desc)) / datediff(ss, lead([Collect time]) over (order by [Collect time] desc), [Collect time]) AS [Блокировок в секунду]
from Metrics
where [Collect time] BETWEEN $__timeFrom() AND $__timeTo()
select
    [Collect time],
    [Memory: Available MB] AS [Доступная память в МБ],
    [Log File(s) Used Size (MB)] AS [Размер файла журнала в МБ],
    [Page life expectancy in seconds] AS [Среднее время жизни страницы в кэше в секундах],
    [Granted Workspace Memory (MB)] AS [Память выделенная под запросы в МБ],
    [Page Cache Size (MB)] AS [Размер кэша страниц в МБ]
from Metrics
where [Collect time] BETWEEN $__timeFrom() AND $__timeTo()
select
    [Collect Time],
    start_time,
    transaction_id,
    login_name,
    cpu_time,
    total_elapsed_time,
    logical_reads,
    writes,
    row_count,
    command,
    SqlQuery
from RunningQueries
where [Collect time] BETWEEN $__timeFrom() AND $__timeTo()
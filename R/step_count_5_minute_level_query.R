#' Average step count over all 5-minute-intervals query
# 
#' @return 
#' a data.table with the following columns:
#' person_id, minute_interval, average_step_count, p_has_steps
#' p is the probability of the interval having > 0 steps.
#' 
#' @examples 
#' /dontrun{
#' step_dat <- step_count_average_5_minute_interval_query()
#' }
#' @export
#' @details Average step count over all 5-minute intervals are computed for each person_id.
step_count_average_5_minute_interval_query <- function()
{
    dest <- "step_count_average_5_minute_interval_query_result.csv"
    query <- stringr::str_glue(
            "
            SELECT person_id,
                minute_interval,
                AVG(steps_sum) AS average_step_count,
                AVG(has_steps) AS p_has_steps
            FROM (SELECT
                    person_id,
                    CAST(datetime AS DATE) AS date,
                    minute_interval,
                    SUM(steps) AS steps_sum,
                    IF(SUM(steps) > 0, 1, 0) as has_steps,
                    IF(COUNT(*)>=5,1,0) AS valid_interval
                FROM (SELECT person_id,
                            datetime,
                            steps,
                            FLOOR((EXTRACT(MINUTE FROM datetime) + 60 * EXTRACT(HOUR FROM datetime)) / 5) AS minute_interval
                    FROM steps_intraday
                )
                GROUP BY person_id, date, minute_interval
                HAVING valid_interval = 1
            )
            GROUP BY person_id, minute_interval
        ")
    result_all <- download_big_data(query,dest,FALSE)
    return(result_all)
}
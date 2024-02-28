#' Cadence pattern query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, date, non_movement,incidental_movement,sporadic_movement,purposeful_movement,slow_walking,medium_walking,brisk_walking,all_faster_ambulation
#' 
#' @examples 
#' /dontrun{
#' dat <- cadence_pattern()
#' }
#' @export
#' @details Non-movement (minutes/day) = 0 steps/min but has heart rate data for that minute
#' Incidental movement (minutes/day) = 1-19 steps/min *
#' Sporadic movement(minutes/day) = 20-39 steps/min
#' Purposeful movement (minutes/day) = 40-59 steps/min
#' Slow walking (minutes/day) = 60-79 steps/min
#' Medium walking (minutes/day) = 80-99 steps/min
#' Brisk walking (minutes/day) = 100-119 steps/min
#' All faster ambulation (minutes/day) = 120+ steps/min
cadence_pattern_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "cadence_pattern_query_result.csv"
    query <- "
        WITH sleep_table AS (
            SELECT person_id, 
                end_sleep, 
                LEAD(start_sleep) OVER(PARTITION BY person_id ORDER BY start_sleep) AS next_sleep,
            FROM
            (
                SELECT person_id,
                    MIN(start_datetime) AS start_sleep,
                    MAX(DATETIME_ADD(start_datetime, INTERVAL cast(duration_in_min*60 AS INT64) SECOND)) AS end_sleep,
                FROM
                    sleep_level
                WHERE 
                    is_main_sleep = 'true'
                GROUP BY person_id, sleep_date
            )
        )
        SELECT * FROM 
        (
            SELECT person_id, cast(datetime AS DATE) as date, cadence, COUNT(cadence) as count
            FROM 
            (
                SELECT s.person_id, s.datetime, s.steps, h.heart_rate_value,
                CASE 
                    WHEN steps = 0 THEN 'non_movement'
                    WHEN steps BETWEEN 1 AND 19 THEN 'incidental_movement' 
                    WHEN steps BETWEEN 20 AND 39 THEN 'sporadic_movement'
                    WHEN steps BETWEEN 40 AND 59 THEN 'purposeful_movement'
                    WHEN steps BETWEEN 60 AND 79 THEN 'slow_walking'
                    WHEN steps BETWEEN 80 AND 99 THEN 'medium_walking'
                    WHEN steps BETWEEN 100 AND 119 THEN 'brisk_walking'
                    ELSE 'all_faster_ambulation' END AS cadence,
                FROM steps_intraday s
                INNER JOIN heart_rate_minute_level h ON (s.datetime = h.datetime AND s.person_id = h.person_id)
                INNER JOIN sleep_table st ON (st.person_id = s.person_id AND 
                                            s.datetime BETWEEN st.end_sleep AND st.next_sleep)
                WHERE h.heart_rate_value > 0 
            )
            GROUP BY person_id, cadence, date
        )
        PIVOT (SUM(count) FOR cadence IN ('non_movement','incidental_movement',
        'sporadic_movement','purposeful_movement','slow_walking','medium_walking',
        'brisk_walking','all_faster_ambulation'))
        "
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"date",anchor_date_table,before,after)
    return(result_all)
}
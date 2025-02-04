#' Approximate resting heart rate query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, date, approx_resting_heart_rate
#' 
#' @examples 
#' /dontrun{
#' hr_dat <- approx_resting_heart_rate_query()
#' }
#' @export
#' @details Looks for HR when there is no significant steps/movemet for 10 mins, and then takes 10th percentile of that set of observed values.
approx_resting_heart_rate_query <- function(anchor_date_table=NULL,before=NULL,after=NULL,cohort=NULL)
{
    dest <- "approx_resting_heart_rate_query_result.csv"
    if (!is.null(cohort))
    {
        cohort <- paste(cohort, collapse = ",")
        cohort <- paste0("(", cohort, ")")
        query <- stringr::str_glue( 
            "WITH step_tb AS 
            (
                SELECT person_id, 
                    CAST(datetime AS DATE) AS date, 
                    IF(COUNT(*)=10,1,0) AS valid_interval,
                    FLOOR((EXTRACT(MINUTE FROM datetime) + 60 * EXTRACT(HOUR FROM datetime)) / 10) AS minute_interval
                FROM steps_intraday
                WHERE steps = 0 AND 
                      person_id IN {cohort}
                GROUP BY person_id, date, minute_interval
                HAVING valid_interval = 1
            ),
            hr_tb AS 
            (
                SELECT person_id, 
                    CAST(datetime AS DATE) AS date, 
                    heart_rate_value,
                    FLOOR((EXTRACT(MINUTE FROM datetime) + 60 * EXTRACT(HOUR FROM datetime)) / 10) AS minute_interval
                FROM heart_rate_minute_level
                WHERE person_id IN {cohort}
            )
            SELECT s.person_id, 
                   s.date,
                   APPROX_QUANTILES(heart_rate_value, 100)[OFFSET(10)] AS approx_resting_heart_rate
            FROM step_tb s
            INNER JOIN hr_tb h ON (s.person_id = h.person_id AND 
                                   s.date = h.date           AND 
                                   s.minute_interval = h.minute_interval)
            GROUP BY s.person_id, s.date"
        )
    } else {
        query <- stringr::str_glue( 
            "WITH step_tb AS 
            (
                SELECT person_id, 
                    CAST(datetime AS DATE) AS date, 
                    IF(COUNT(*)=10,1,0) AS valid_interval,
                    FLOOR((EXTRACT(MINUTE FROM datetime) + 60 * EXTRACT(HOUR FROM datetime)) / 10) AS minute_interval
                FROM steps_intraday
                WHERE steps = 0
                GROUP BY person_id, date, minute_interval
                HAVING valid_interval = 1
            ),
            hr_tb AS 
            (
                SELECT person_id, 
                    CAST(datetime AS DATE) AS date, 
                    heart_rate_value,
                    FLOOR((EXTRACT(MINUTE FROM datetime) + 60 * EXTRACT(HOUR FROM datetime)) / 10) AS minute_interval
                FROM heart_rate_minute_level
            )
            SELECT s.person_id, 
                   s.date,
                   APPROX_QUANTILES(heart_rate_value, 100)[OFFSET(10)] AS approx_resting_heart_rate
            FROM step_tb s
            INNER JOIN hr_tb h ON (s.person_id = h.person_id AND 
                                   s.date = h.date           AND 
                                   s.minute_interval = h.minute_interval)
            GROUP BY s.person_id, s.date"
        )
    }
    result_all <- download_big_data(query,dest,FALSE)
    result_all <- window_data(result_all,"date",anchor_date_table,before,after)
    return(result_all)
}
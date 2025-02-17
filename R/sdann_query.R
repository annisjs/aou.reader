#' SDANN query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, sdann_date, sdann_value, sdann_total_valid_interval
#' 
#' @examples 
#' /dontrun{
#' sdann_dat <- sdann_query()
#' }
#' @export
#' @details Heart rate minute level data is extracted and five-minute averages are calculated for consecutive five-minute intervals.
#' The average HR is used to calculate the average RR duration of each five-minute interval:
#' Average RR = 6000 / mean(HR)
#' Subsequently, the standard deviation of all the five-minute RR intervals is calculated, yielding the SDANN value (in ms).
sdann_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "sdann_query_result.csv"
    query <- stringr::str_glue(
        "
        SELECT person_id,
            sdann_date,
            STDDEV(avg_rr) AS sdann_value,
            SUM(valid_interval) AS sdann_total_valid_interval
        FROM (SELECT
                person_id,
                CAST(datetime AS DATE) AS sdann_date,
                6000 / AVG(heart_rate_value) AS avg_rr,
                IF(COUNT(*)>=5,1,0) AS valid_interval
            FROM (SELECT person_id,
                        datetime,
                        heart_rate_value,
                        FLOOR((EXTRACT(MINUTE FROM datetime) + 60 * EXTRACT(HOUR FROM datetime)) / 5) AS minute_interval
                FROM heart_rate_minute_level
            )
            GROUP BY person_id, sdann_date, minute_interval
            HAVING valid_interval = 1
        )
        GROUP BY person_id, sdann_date
    ")
    result_all <- download_big_data(query,dest,FALSE)
    result_all <- window_data(result_all,"sdann_date",anchor_date_table,before,after)
    return(result_all)
}
#' Wear time query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, condition_start_date, condition_source_value
#' 
#' @details  An hour of wear time is defined when step count is > 0 for a given hour of the day,
#' 
#' @examples
#' \dontrun{
#' wear_time_date <- wear_time_query()
#' }
#' @export 
wear_time_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "wear_time_query.csv"
    query <- stringr::str_glue("
            SELECT person_id, date, SUM(has_hour) AS wear_time
            FROM (SELECT person_id, CAST(datetime AS DATE) AS date, IF(SUM(steps)>0, 1, 0) AS has_hour
                    FROM `steps_intraday`
                    GROUP BY CAST(datetime AS DATE), EXTRACT(HOUR FROM datetime), person_id) t
            GROUP BY date, person_id
    ")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"date",anchor_date_table,before,after)
    return(result_all)
}
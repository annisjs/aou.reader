#' Wear time HR query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, date, wear_time_hr
#' 
#' @details  All minutes were HR > 0 are summed for each person_id and date
#' 
#' @examples
#' \dontrun{
#' wear_time_hr_dat <- wear_time_hr_query()
#' }
#' @export 
wear_time_hr_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "wear_time_hr_query.csv"
    query <- stringr::str_glue("
                SELECT person_id, CAST(datetime AS DATE) AS date, COUNT(*) AS wear_time_hr
                FROM heart_rate_minute_level
                WHERE heart_rate_value > 0 AND
                GROUP BY person_id, date
    ")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"date",anchor_date_table,before,after)
    return(result_all)
}
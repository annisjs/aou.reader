 #' Sleep level query
#' 
#' @param sleep_terms a SQL WHERE statement for filtering sleep levels. If no statement is provided, then no filtering is performed.
#' @param time which time should be pulled? Can be \code{first}, \code{last}, or \code{all}. Note, \code{all} will return all sleep levels, which can be quite large. Default is \code{first}.
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, sleep_level_date, sleep_level_datetime, sleep_level_duration, sleep_level_is_main_sleep
#' 
#' @examples 
#' /dontrun{
#' sleep_dat <- sleep_query("is_main_sleep = TRUE AND level = 'deep'") #first deep/main sleep datetimes
#' }
#' @export 
sleep_level_query <- function(sleep_terms,time="first",anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "sleep_level_query_result.csv"
    if (time=="first") 
    {
        ordering <- "asc"
    } else if (time == "last") {
        ordering <- "desc"
    } else if (time != "all") {
        stop("time must be first, last, or all")
    }

    if (!missing(sleep_terms))
    {
        sleep_terms <- paste("WHERE",sleep_terms)
    } else {
        sleep_terms <- ""
    }

    if (time == "all")
    {
        query <- stringr::str_glue("
                SELECT person_id,
                    sleep_date AS sleep_date,
                    start_datetime AS sleep_datetime,
                    duration_in_min AS sleep_duration,
                    is_main_sleep AS sleep_is_main_sleep
                FROM
                    `sleep_level` sleep_level
                {sleep_terms}", sep="")
    } else {
        query <- stringr::str_glue("
                SELECT person_id,
                    sleep_date AS sleep_date,
                    start_datetime AS sleep_datetime,
                    duration_in_min AS sleep_duration,
                    is_main_sleep AS sleep_is_main_sleep
        FROM (SELECT person_id, sleep_date, start_datetime, duration_in_min, is_main_sleep,
                row_number() over(partition by person_id, sleep_date order by start_datetime {ordering}) as rn
                FROM sleep_level
                {sleep_terms}) as t1
        WHERE rn = 1")
    }
    result_all <- download_big_data(query,dest,FALSE)
    result_all <- window_data(result_all,"sleep_date",anchor_date_table,before,after)
    return(result_all)
}
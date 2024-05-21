#' Hourly steps query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' @param cohort a vector or list of integers indicating the person_id to be queried.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, date, hour, hourly_steps_count
#' 
#' @details  Returns the number of steps at each day at each hour
#' 
#' @examples
#' \dontrun{
#' hourly_steps_df <- hourly_steps_query()
#' }
#' @export 
hourly_steps_query <- function(anchor_date_table=NULL,before=NULL,after=NULL, cohort=NULL)
{
  dest <- "hourly_steps_query.csv"
  if(is.null(cohort)){
    query <- stringr::str_glue("
            SELECT person_id, CAST(datetime AS DATE) AS date, SUM(steps) AS hourly_steps_count, EXTRACT(HOUR FROM datetime) as hour
                    FROM `steps_intraday`
                    GROUP BY CAST(datetime AS DATE), EXTRACT(HOUR FROM datetime), person_id
    ") 
  }else{
    cohort <- paste(cohort, collapse = ",")
    cohort <- paste0("(", cohort, ")")
    query <- stringr::str_glue("
            SELECT person_id, CAST(datetime AS DATE) AS date, SUM(steps) AS hourly_steps_count, EXTRACT(HOUR FROM datetime) as hour
                    FROM `steps_intraday` WHERE person_id IN {cohort}  
                    GROUP BY CAST(datetime AS DATE), EXTRACT(HOUR FROM datetime), person_id
    ")
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"date",anchor_date_table,before,after)
  return(result_all)
}
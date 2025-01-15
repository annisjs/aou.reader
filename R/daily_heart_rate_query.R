#' Daily heart rate query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' @param cohort a vector or list of integers indicating the person_id to be queried.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, date, mean_daily_heart_rate, sd_daily_heart_rate
#' 
#' @details  Returns the average and standard deviation of heart rate at each day. 
#' 
#' @examples
#' \dontrun{
#' hr_df <- daily_heart_rate_query()
#' }
#' @export 
daily_heart_rate_query <- function(anchor_date_table=NULL,before=NULL,after=NULL, cohort=NULL)
{
  dest <- "daily_heart_rate_query.csv"
  if(is.null(cohort)){
     query <- stringr::str_glue("
            SELECT person_id, 
                    CAST(datetime AS DATE) AS date,
                    AVG(heart_rate_value) AS mean_daily_heart_rate, 
                    STDDEV(heart_rate_value) AS sd_daily_heart_rate
            FROM heart_rate_minute_level 
            GROUP BY CAST(datetime AS DATE), person_id
    ") 
  }else{
    cohort <- paste(cohort, collapse = ",")
    cohort <- paste0("(", cohort, ")")
    query <- stringr::str_glue("
            SELECT person_id, 
                    CAST(datetime AS DATE) AS date,
                    AVG(heart_rate_value) AS mean_daily_heart_rate, 
                    STDDEV(heart_rate_value) AS sd_daily_heart_rate
            FROM heart_rate_minute_level 
            WHERE person_id IN {cohort}  
            GROUP BY CAST(datetime AS DATE), person_id
    ") 
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"date",anchor_date_table,before,after)
  return(result_all)
}
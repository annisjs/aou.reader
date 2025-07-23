#'  Heart rate summary query
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
#' @details  Returns person_id, date, zone_name, min_heart_rate, max_heart_rate, minute_in_zone
#' 
#' @examples
#' \dontrun{
#' hr_df <- heart_rate_summary_query()
#' }
#' @export 
heart_rate_summary_query <- function(anchor_date_table=NULL,before=NULL,after=NULL, cohort=NULL)
{
        dest <- "heart_rate_summary_query.csv"
        query <- paste("
                SELECT
                        heart_rate_summary.person_id,
                        heart_rate_summary.date,
                        heart_rate_summary.zone_name,
                        heart_rate_summary.min_heart_rate,
                        heart_rate_summary.max_heart_rate,
                        heart_rate_summary.minute_in_zone
                FROM
                        heart_rate_summary", sep="")
        result_all <- download_big_data(query,dest,FALSE)
        result_all <- window_data(result_all,"date",anchor_date_table,before,after)
        return(result_all)
}
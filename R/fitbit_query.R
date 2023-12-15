
#' Fitbit query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, date, steps, fairly_active_minutes, lightly_active_minutes, sedentary_minutes, very_active_minutes
#' 
#' @examples 
#' /dontrun{
#' bmi_dat <- bmi_query()
#' }
#' @export
fitbit_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "fitbit_query.csv"
    query <- paste("
            SELECT
                activity_summary.person_id,
                activity_summary.date,
                activity_summary.steps,
                activity_summary.fairly_active_minutes,
                activity_summary.lightly_active_minutes,
                activity_summary.sedentary_minutes,
                activity_summary.very_active_minutes
            FROM
                `activity_summary` activity_summary", sep="")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"date",anchor_date_table,before,after)
    return(result_all)
}

#' Sleep data from Fitbit query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, sleep_date, is_main_sleep, minute_in_bed, minute_asleep, 
#' minute_after_wakeup, minute_awake, minute_restless, 
#' minute_deep, minute_light, minute_rem, minute_wake
#' 
#' @examples 
#' /dontrun{
#' sleep_dat <- sleep_query()
#' }
#' @export
sleep_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
	dest <- "sleep_query_result"
	query <- paste("
			SELECT
			sleep_daily_summary.person_id
			, sleep_daily_summary.sleep_date
			, sleep_daily_summary.is_main_sleep
			, sleep_daily_summary.minute_in_bed
			, sleep_daily_summary.minute_asleep
			, sleep_daily_summary.minute_after_wakeup
			, sleep_daily_summary.minute_awake
			, sleep_daily_summary.minute_restless
			, sleep_daily_summary.minute_deep
			, sleep_daily_summary.minute_light
			, sleep_daily_summary.minute_rem
			, sleep_daily_summary.minute_wake
			FROM
				`sleep_daily_summary` sleep_daily_summary", sep="")
	result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"sleep_date",anchor_date_table,before,after)
    return(result_all)
}
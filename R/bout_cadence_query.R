#' Bout cadence query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#
#' @return 
#' a data.table with the following columns:
#' person_id, bout_cadence_date, bout_cadence_value
#' 
#' @details
#' Bout cadence is the average steps per minute when the step count >/= 60 steps a minute for at least 2 minutes.
#' The wearer will usually have many of these “bouts” throughout the day.
#' We take the average over the entire day to get the average bout cadence.
#' 
#' @examples
#' \dontrun{
#' bout_dat <- bout_cadence_query()
#' }
#' @export
bout_cadence_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
  	dest <- "bout_cadence_query_result.csv"
	query <- stringr::str_glue("
			SELECT person_id,
				CAST(datetime AS DATE) as bout_cadence_date,
				AVG(steps) as bout_cadence_value
			FROM (SELECT steps_intraday.*,
						lag (datetime) over (partition by person_id, CAST(datetime AS DATE) order by datetime) as nextTimestamp_lag,
						lead (datetime) over (partition by person_id, CAST(datetime AS DATE) order by datetime) as nextTimestamp_lead
				from steps_intraday
				where steps >= 60
				) t
			WHERE
			(DATE_DIFF(datetime,nextTimestamp_lag,minute) <= 1 OR
			DATE_DIFF(nextTimestamp_lead,datetime,minute) <= 1)
			GROUP BY
			CAST(datetime AS DATE),person_id
		")
	result_all <- download_big_data(query,dest,FALSE)
	result_all <- window_data(result_all,"bout_cadence_date",anchor_date_table,before,after)
	return(result_all)
}
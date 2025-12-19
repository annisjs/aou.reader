
#' Device query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, device_type, device_model, data_record_date, last_sync_time, last_sync_date
#' anchored by last sync date
#' @examples 
#' /dontrun{
#' device_dat <- device_query()
#' }
#' @export
device_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "device_query.csv"
    query <- paste("
            SELECT
               person_id,
               device_type,
               device_version AS device_model,
               device_date AS data_record_date,
               last_sync_time,
               CAST(last_sync_time AS DATE) AS last_sync_date
            FROM
                device", sep="")
    result_all <- download_big_data(query,dest,FALSE)
    result_all <- window_data(result_all,"last_sync_date",anchor_date_table,before,after)
    return(result_all)
}
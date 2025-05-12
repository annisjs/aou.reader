
#' Wear study query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, wear_study_consent, wear_study_consent_start_date, wear_study_consent_end_date
#' 
#' @examples 
#' /dontrun{
#' wear_study_dat <- wear_study_query()
#' }
#' @export
wear_study_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "wear_study_query.csv"
    query <- paste("
            SELECT
               resultsconsent_wear AS wear_study_consent,
               wear_consent_start_date AS wear_study_consent_start_date,
               wear_consent_end_date AS wear_study_consent_end_date
            FROM
                wear_study", sep="")
    result_all <- download_big_data(query,dest,FALSE)
    result_all <- window_data(result_all,"wear_study_consent_start_date",anchor_date_table,before,after)
    return(result_all)
}
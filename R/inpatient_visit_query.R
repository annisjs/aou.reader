#' Inpatient visit query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, inpatient_visit_date
#' 
#' @details 
#' Provides the date of an inpatient visit.
#' 
#' @examples
#' \dontrun{
#' inpatient_visit dat <- inpatient_visit_query()
#' }
#' @export 
inpatient_visit_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "inpatient_visit_query_result.csv"
    query <- stringr::str_glue(
        "SELECT person_id,visit_start_date
            FROM
               visit_occurrence
            WHERE
                visit_concept_id IN (9201,9203)
            ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"visit_start_date",anchor_date_table,before,after)
  return(result_all)
}
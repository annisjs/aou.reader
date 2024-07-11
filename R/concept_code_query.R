#' Concept code query
#' 
#' @param codes a character vector or string containing medication names
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table containing the following columns:
#' person_id, condition_start_date, concept_name, condition_concept_id
#' @examples
#' \dontrun{
#' concept_code_dat <- concept_code_query(c(441641,4014295))
#' }
#' @export
concept_code_query <- function(codes,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "concept_code_query_result.csv"
  codes <- paste0("(",paste0(codes,collapse=","),")")
  query <- str_glue("
        SELECT person_id, condition_start_date, concept_name, condition_concept_id
        FROM condition_occurrence co
        INNER JOIN concept c ON (co.condition_concept_id = c.concept_id)
        WHERE condition_concept_id IN {codes}
  ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"condition_start_date",anchor_date_table,before,after)
  return(result_all)
}
#' Condition query
#' 
#' @param concept_ids a numeric vector with condition concept ids
#' @param source_values a character vector with condition source values
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return
#' a data.table containing the following columns:
#' person_id, condition_start_date, condition_concep_id, condition_source_value
#' 
#' @examples
#' \dontrun{
#' condition_dat <- condition_query(concept_ids = c(710706,705076), source_values = c("U09.9"))
#' }
#' @export
condition_query <- function(concept_ids=NULL,source_values=NULL, anchor_date_table=NULL,before=NULL,after=NULL)
{
  if(is.null(concept_ids) & is.null(source_values)){
    stop("Both concept ids and source values can't be null")
  }
  dest <- "condition_query_result.csv"
  if(is.null(concept_ids)){
    dx_values <- paste('c.condition_source_value LIKE ',"'",source_values,"'",collapse=' OR ',sep="")
    query <- stringr::str_glue("
            SELECT person_id, condition_start_date, condition_source_value, condition_concept_id
            FROM `condition_occurrence` c
            WHERE ({dx_values})")
  }else if(is.null(source_values)){
    dx_ids <- paste(concept_ids, collapse=', ')
    query <- stringr::str_glue("
            SELECT person_id, condition_start_date, condition_source_value, condition_concept_id
            FROM `condition_occurrence` c WHERE condition_concept_id IN ({dx_ids})")
  }else{
    dx_ids <- paste(concept_ids, collapse=', ')
    dx_values <- paste('c.condition_source_value LIKE ',"'",source_values,"'",collapse=' OR ',sep="")
    query <- stringr::str_glue("
            SELECT person_id, condition_start_date, condition_source_value, condition_concept_id
            FROM `condition_occurrence` c WHERE ({dx_values}) OR condition_concept_id IN ({dx_ids})")
    
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"condition_start_date",anchor_date_table,before,after)
  return(result_all)
}
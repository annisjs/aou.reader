#' Lab concept query
#' 
#' @param labs a character vector or string containing the labs concepts to query
#' @param value_as_number a numeric vector to subset labs by values as number
#' @param value_as_concept_id a numeric vector to subset labs by values as concept ids
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return
#' a data.table containing the following columns:
#' person_id, measurement_date, value_as_number, value_as_concept
#' 
#' @examples
#' \dontrun{
#' lab_concept_dat <- lab_concept_query(c(586520,586523,586525))
#' }
#' @export 
lab_concept_query <- function(lab_concepts, value_as_number=NULL, value_as_concept_id=NULL, anchor_date_table=NULL,before=NULL,after=NULL)
{
  if(!is.null(value_as_number) & !is.null(value_as_concept_id)){
    stop("Only value as a number or value as a concept id can be set for subsetting.")
  }
  dest <- "lab_concept_query_result.csv"
  lab_concepts <- paste(lab_concepts,collapse=', ')
  query <- stringr::str_glue("
        SELECT person_id, measurement_date, value_as_number, value_as_concept_id
        FROM `measurement` m
        INNER JOIN `concept` c ON (m.measurement_concept_id = c.concept_id)
        WHERE c.concept_id IN ({lab_concepts})
        ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
  if(!is.null(value_as_number)){
    result_all = subset(result_all, value_as_number %in% value_as_number & !is.na(value_as_number))
  } else if(!is.null(value_as_concept_id)){
    result_all = subset(result_all, value_as_concept_id %in% value_as_concept_id & !is.na(value_as_concept_id))
  } 
  return(result_all)
}

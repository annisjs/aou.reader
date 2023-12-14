#' Lab query
#' 
#' @param labs a character vector or string containing the labs to query
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return
#' a data.table containing the following columns:
#' person_id, measurement_date, value_as_number
#' 
#' @examples
#' \dontrun{
#' lab_dat <- lab_query(c("Triglyceride [Mass/volume] in Serum or Plasma","Triglyceride [Mass/volume] in Blood"))
#' }
#' @export 
lab_query <- function(labs,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  dest <- "lab_query_result.csv"
  lab_terms <- paste('c.concept_name LIKE ',"'",labs,"'",collapse=' OR ',sep="")
  query <- stringr::str_glue("
        SELECT person_id, measurement_date, value_as_number
        FROM `{dataset}.measurement` m
        INNER JOIN `{dataset}.concept` c ON (m.measurement_concept_id = c.concept_id)
        WHERE
        ({lab_terms})
        ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
  return(result_all)
}
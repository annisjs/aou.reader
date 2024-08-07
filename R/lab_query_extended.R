#' Lab query with extended columns
#' 
#' @param labs a character vector or string containing the labs to query
#' @param ext_cols extra columns to return on top of the date and value of the lab
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return
#' a data.table containing the following columns:
#' person_id, measurement_date, value_as_number, ext_cols
#' 
#' @examples
#' \dontrun{
#' lab_dat <- lab_query(c("Triglyceride [Mass/volume] in Serum or Plasma","Triglyceride [Mass/volume] in Blood"), ext_cols = c("unit_source_value"))
#' }
#' @export 
lab_query_extended <- function(labs,ext_cols=NULL,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "lab_query_result.csv"
  lab_terms <- paste('c.concept_name LIKE ',"'",labs,"'",collapse=' OR ',sep="")
  if(is.null(ext_cols)){
    query <- stringr::str_glue("
        SELECT person_id, measurement_date, value_as_number
        FROM `measurement` m
        INNER JOIN `concept` c ON (m.measurement_concept_id = c.concept_id)
        WHERE
        ({lab_terms})
        ")  
  }else{
    extr_cols = paste(ext_cols, collapse = ", ")
    query <- stringr::str_glue("
        SELECT person_id, measurement_date, value_as_number, {extr_cols}
        FROM `measurement` m
        INNER JOIN `concept` c ON (m.measurement_concept_id = c.concept_id)
        WHERE
        ({lab_terms})
        ")
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
  return(result_all)
}
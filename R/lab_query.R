#' Lab query
#' 
#' @param labs a character vector or string containing the labs to query
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
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
lab_query <- function(labs,page_size=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  lab_terms <- paste('c.concept_name LIKE ',"'",labs,"'",collapse=' OR ',sep="")
  query <- str_glue("
        SELECT person_id, measurement_date, value_as_number
        FROM `{dataset}.measurement` m
        INNER JOIN `{dataset}.concept` c ON (m.measurement_concept_id = c.concept_id)
        WHERE
        ({lab_terms})
        ")
  download_data(query,page_size)
}
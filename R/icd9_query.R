#' ICD9 Query
#'
#' @param icd9_codes a character vector or character string containing ICD9 codes. String can contain wildcards using % (e.g. "410.%").
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#'
#' @return
#' a data.table with the following columns:
#' person_id, condition_start_date, condition_source_value
#' @export
#'
#' @examples
#' \dontrun{
#' icd9_dat <- icd9_codes(c("410","410.%"))
#' }
icd9_query <- function(icd9_codes,page_size=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  icd9_terms <- paste('co.CONDITION_SOURCE_VALUE LIKE ',"'",icd9_codes,"'",collapse=' OR ',sep="")
  query <-str_glue("
    SELECT DISTINCT co.person_id, co.condition_start_date,co.condition_source_value
    FROM
        {dataset}.condition_occurrence co
        INNER JOIN
        {dataset}.concept c
        ON (co.condition_source_concept_id = c.concept_id)
    WHERE
        c.VOCABULARY_ID LIKE 'ICD9CM' AND
        ({icd9_terms})
    ")
  download_data(query,page_size)
}
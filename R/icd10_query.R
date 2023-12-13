#' ICD10 Query
#'
#' @param icd9_codes a character vector or character string containing ICD10 codes. String can contain wildcards using % (e.g. "410.%").
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#'
#' @return
#' a data.table with the following columns:
#' person_id, condition_start_date, condition_source_value
#' @export
#'
#' @examples
#' \dontrun{
#' icd10_dat <- icd10_codes(c("I21","I21.%"))
#' }
icd10_query <- function(dataset,icd10_codes,page_size=NULL)
{
  icd10_terms <- paste('co.CONDITION_SOURCE_VALUE LIKE ',"'",icd10_codes,"'",collapse=' OR ',sep="")
  query <-  str_glue("
        SELECT DISTINCT co.person_id,co.condition_start_date,co.condition_source_value
        FROM
        {dataset}.condition_occurrence co
        INNER JOIN
        {dataset}.concept c
        ON (co.condition_source_concept_id = c.concept_id)
    WHERE
        c.VOCABULARY_ID LIKE 'ICD10CM' AND
        ({icd10_terms})
    ")
  download_data(query,page_size)
}

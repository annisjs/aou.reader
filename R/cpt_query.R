#' CPT Query
#'
#' @param cpt_codes a character vector or character string of CPT codes
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#'
#' @return
#' A data.table with the following columns:
#' person_id, cpt_code, entry_date
#' @export
#'
#' @examples
#' \dontrun{
#' cpt_dat <- cpt_query(c("33510","33511"))
#' }
cpt_query <- function(cpt_codes,page_size=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  cpt_terms <- paste('c.CONCEPT_CODE LIKE ',"'",cpt_codes,"'",collapse=' OR ',sep="")
  query <- str_glue("
    SELECT DISTINCT p.person_id,c.CONCEPT_CODE AS cpt_code,p.PROCEDURE_DATE AS entry_date
    FROM
        {dataset}.concept c,
        {dataset}.procedure_occurrence p
        WHERE
        c.VOCABULARY_ID like 'CPT4' AND
        c.CONCEPT_ID = p.PROCEDURE_SOURCE_CONCEPT_ID AND
        ({cpt_terms})
    ")
  download_data(query,page_size)
}
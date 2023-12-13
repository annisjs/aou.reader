#' Inpatient ICD query
#' @param codes a character vector or string of ICD9/10 codes
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, condition_start_date, condition_source_value
#' 
#' @details 
#' Matches provided ICD9/10 code 
#' AND 
#' (
#'    Can be any position on chart. Captured by condition_type_concept_id in condition_occurrence table
#' OR 
#'    Is an inpatient visit. Captured by the visit_concept_id in the visit_occurrence table
#' )
#' 
#' @export 
#' @examples
#' \dontrun{
#' inpatient_dat <- inpatient_icd_query(c("I21","I21.%"))
#' }
inpatient_icd_query <- function(codes,page_size=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  code_clause <- paste('co.CONDITION_SOURCE_VALUE LIKE ',"'",codes,"'",collapse=' OR ',sep="")
  query <- str_glue(
    "SELECT co.person_id,co.condition_start_date,co.condition_source_value
        FROM
            `{dataset}.condition_occurrence` co
            LEFT JOIN
            `{dataset}.concept` c
            ON (co.condition_source_concept_id = c.concept_id)
            LEFT JOIN
            `{dataset}.visit_occurrence` v
            ON (co.visit_occurrence_id = v.visit_occurrence_id)
        WHERE
            c.vocabulary_id LIKE 'ICD%' AND
            ({code_clause}) AND
            (co.condition_type_concept_id IN (38000200,38000201,
            38000202,38000203,38000204,38000205,38000214,
            38000206,38000207,38000208,38000209,38000210,
            38000211,38000212,38000213) OR
            v.visit_concept_id = 9201)
        ")
  download_data(query,page_size)
}
#' Inpatient ICD query
#' 
#' @param codes a character vector or string of ICD9/10 codes
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
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
#' @examples
#' \dontrun{
#' inpatient_dat <- inpatient_icd_query(c("I21","I21.%"))
#' all_inpatient_dat <- inpatient_icd_query() # if no codes given, all will be returned.
#' }
#' @export 
inpatient_icd_query <- function(codes=NULL,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "inpatient_icd_query_result.csv"
  code_clause <- paste('co.CONDITION_SOURCE_VALUE LIKE ',"'",codes,"'",collapse=' OR ',sep="")
  if (is.null(codes))
  {
    query <- stringr::str_glue(
        "SELECT co.person_id,co.condition_start_date,co.condition_source_value
            FROM
                `condition_occurrence` co
                LEFT JOIN
                `concept` c
                ON (co.condition_source_concept_id = c.concept_id)
                LEFT JOIN
                `visit_occurrence` v
                ON (co.visit_occurrence_id = v.visit_occurrence_id)
            WHERE
                c.vocabulary_id LIKE 'ICD%' AND
                (co.condition_type_concept_id IN (38000200,38000201,
                38000202,38000203,38000204,38000205,38000214,
                38000206,38000207,38000208,38000209,38000210,
                38000211,38000212,38000213) OR
                v.visit_concept_id IN (9201,9203))
            ")
  } else {
    query <- stringr::str_glue(
        "SELECT co.person_id,co.condition_start_date,co.condition_source_value
            FROM
                `condition_occurrence` co
                LEFT JOIN
                `concept` c
                ON (co.condition_source_concept_id = c.concept_id)
                LEFT JOIN
                `visit_occurrence` v
                ON (co.visit_occurrence_id = v.visit_occurrence_id)
            WHERE
                c.vocabulary_id LIKE 'ICD%' AND
                ({code_clause}) AND
                (co.condition_type_concept_id IN (38000200,38000201,
                38000202,38000203,38000204,38000205,38000214,
                38000206,38000207,38000208,38000209,38000210,
                38000211,38000212,38000213) OR
                v.visit_concept_id IN (9201,9203))
            ")
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"condition_start_date",anchor_date_table,before,after)
  return(result_all)
}
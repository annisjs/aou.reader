#' ICD10 Query
#'
#' @param icd10_codes a character vector or character string containing ICD10 codes. String can contain wildcards using % (e.g. "410.%").
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#'
#' @return
#' a data.table with the following columns:
#' person_id, condition_start_date, condition_source_value
#'
#' @examples
#' \dontrun{
#' icd10_dat <- icd10_query(c("I21","I21.%"))
#' }
#' @export
icd10_query <- function(icd10_codes=NULL,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "icd10_query_result.csv"
  icd10_terms <- paste('co.CONDITION_SOURCE_VALUE LIKE ',"'",icd10_codes,"'",collapse=' OR ',sep="")
  if (!is.null(icd10_codes))
  {
    query <-  stringr::str_glue("
          SELECT DISTINCT co.person_id,co.condition_start_date,co.condition_source_value
          FROM
          condition_occurrence co
          INNER JOIN
          concept c
          ON (co.condition_source_concept_id = c.concept_id)
      WHERE
          c.VOCABULARY_ID LIKE 'ICD10CM' AND
          ({icd10_terms})
      ")
  } else {
    query <- stringr::str_glue("
          SELECT DISTINCT co.person_id,
            MIN(co.condition_start_date) AS condition_start_date,
            co.condition_source_value AS condition_source_value,
            COUNT(DISTINCT co.condition_start_date) AS code_count
        FROM
            condition_occurrence co
            INNER JOIN
            concept c
            ON (co.condition_source_concept_id = c.concept_id)
        WHERE
            c.VOCABULARY_ID LIKE 'ICD10CM'
            GROUP BY person_id, condition_source_value
      ")
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"condition_start_date",anchor_date_table,before,after)
  return(result_all)
}

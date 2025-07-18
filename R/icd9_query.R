#' ICD9 Query
#'
#' @param icd9_codes a character vector or character string containing ICD9 codes. String can contain wildcards using % (e.g. "410.%").
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
#' icd9_dat <- icd9_query(c("410","410.%"))
#' }
#' @export
icd9_query <- function(icd9_codes=NULL,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "icd9_query_result.csv"
  icd9_terms <- paste('co.CONDITION_SOURCE_VALUE LIKE ',"'",icd9_codes,"'",collapse=' OR ',sep="")
  if (!is.null(icd9_codes))
  {
    query <- stringr::str_glue("
      SELECT DISTINCT co.person_id, co.condition_start_date,co.condition_source_value
      FROM
          condition_occurrence co
          INNER JOIN
          concept c
          ON (co.condition_source_concept_id = c.concept_id)
      WHERE
          c.VOCABULARY_ID LIKE 'ICD9CM' AND
          ({icd9_terms})
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
        c.VOCABULARY_ID LIKE 'ICD9CM'
    GROUP BY person_id, condition_source_value
  ")
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"condition_start_date",anchor_date_table,before,after)
  return(result_all)
}
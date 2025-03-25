#' ICD Procedure Query
#'
#' @param icd_codes a character vector or character string containing ICD procedure codes. String can contain wildcards using %.
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#'
#' @return
#' a data.table with the following columns:
#' person_id, icd_procedure_date, icd_procedure_code
#'
#' @examples
#' \dontrun{
#' icd_proc_dat <- icd_procedure_query(c("0210%","0211%"))
#' }
#' @export
icd_procedure_query <- function(icd_codes=NULL,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "icd_procedure_query_result.csv"
  icd_terms <- paste('c.concept_code LIKE ',"'",icd_codes,"'",collapse=' OR ',sep="")
  if (!is.null(icd_codes))
  {
    query <-  stringr::str_glue("
          SELECT DISTINCT po.person_id,
                  po.procedure_date AS icd_procedure_date,
                  c.concept_code AS icd_procedure_code
          FROM
          procedure_occurrence po
          INNER JOIN
          concept c
          ON (po.procedure_source_concept_id = c.concept_id)
      WHERE
          c.VOCABULARY_ID LIKE 'ICD%' AND
          ({icd_terms})
      ")
  } else {
    query <- str_glue("
          SELECT DISTINCT po.person_id,
                  MIN(po.procedure_date) AS icd_procedure_date,
                  c.concept_code AS icd_procedure_code
        FROM
            procedure_occurrence po
            INNER JOIN
            concept c
            ON (po.procedure_source_concept_id = c.concept_id)
        WHERE
            c.VOCABULARY_ID LIKE 'ICD%'
            GROUP BY po.person_id, c.concept_code
      ")
  }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"icd_procedure_date",anchor_date_table,before,after)
  return(result_all)
}

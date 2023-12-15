#' CPT Query
#'
#' @param cpt_codes a character vector or character string of CPT codes
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#'
#' @return
#' A data.table with the following columns:
#' person_id, cpt_code, entry_date
#'
#' @examples
#' \dontrun{
#' cpt_dat <- cpt_query(c("33510","33511"))
#' }
#' @export
cpt_query <- function(cpt_codes,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "cpt_query_result.csv"
  cpt_terms <- paste('c.CONCEPT_CODE LIKE ',"'",cpt_codes,"'",collapse=' OR ',sep="")
  query <- stringr::str_glue("
    SELECT DISTINCT p.person_id,c.CONCEPT_CODE AS cpt_code,p.PROCEDURE_DATE AS entry_date
    FROM
        concept c,
        procedure_occurrence p
        WHERE
        c.VOCABULARY_ID like 'CPT4' AND
        c.CONCEPT_ID = p.PROCEDURE_SOURCE_CONCEPT_ID AND
        ({cpt_terms})
    ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"entry_date",anchor_date_table,before,after)
  return(result_all)
}


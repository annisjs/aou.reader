#' Hospitalization query
#' 
#' @param codes a character vector or character string of ICD codes. If NULL, all hospitalizations will be downloaded.
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return a data.table with the following columns: person_id, hospitalization_entry_date, hospitalization_icd_code
#' 
#' @examples 
#' \dontrun{
#' dat <- hospitalization_query("I40")
#' }
#' @export
hospitalization_query <- function(codes=NULL,anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "hospitalization_query_result.csv"
    if (!is.null(codes))
    {
        code_terms <- paste('co.condition_source_value LIKE ',"'",codes,"'",collapse=' OR ',sep="")
        query <- stringr::str_glue("
            SELECT  co.person_id,
                    vo.visit_start_date AS hospitalization_entry_date,
                    co.condition_source_value AS hospitalization_icd_code
            FROM
                `condition_occurrence` co
                LEFT JOIN concept c ON (co.condition_source_concept_id = c.concept_id)
                LEFT JOIN `visit_occurrence` vo ON (co.visit_occurrence_id = vo.visit_occurrence_id)
            WHERE
                c.VOCABULARY_ID LIKE 'ICD%' AND
                (
                    (vo.visit_concept_id = 9201 OR vo.visit_concept_id = 9203) 
                    AND
                    (co.condition_type_concept_id = 38000200 OR co.condition_status_concept_id = 4230359)
                ) AND
                ({code_terms})
    ")
    } else {
        query <- stringr::str_glue("
            SELECT  co.person_id,
                vo.visit_start_date AS hospitalization_entry_date,
                co.condition_source_value AS hospitalization_icd_code
            FROM
                `condition_occurrence` co
                LEFT JOIN
                `concept` c
                ON (co.condition_source_concept_id = c.concept_id)
                LEFT JOIN
                `visit_occurrence` vo
                ON (co.visit_occurrence_id = vo.visit_occurrence_id)
            WHERE
                c.VOCABULARY_ID LIKE 'ICD%' AND
                (
                    (vo.visit_concept_id = 9201 OR vo.visit_concept_id = 9203) 
                    AND
                    (co.condition_type_concept_id = 38000200 OR co.condition_status_concept_id = 4230359)
                ) 
        ")
    }
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"hospitalization_entry_date",anchor_date_table,before,after)
  return(result_all)
}
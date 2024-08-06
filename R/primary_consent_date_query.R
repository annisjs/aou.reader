#' Primary consent date
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' The date someone enrolled into the All of Us Research Program as a data contributing participant
#' a data.table with the following columns:
#' person_id, primary_consent_date
#' 
#' @examples 
#' /dontrun{
#' consent_dat <- primary_consent_date_query()
#' }
#' @export
primary_consent_date_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "death_cause_query_result.csv"
    query = str_glue("
        SELECT DISTINCT
            person_id,
            MIN(observation_date) AS primary_consent_date
            FROM `concept`
            JOIN `concept_ancestor` on concept_id = ancestor_concept_id
            JOIN `observation` on descendant_concept_id = observation_source_concept_id
            WHERE concept_name = 'Consent PII' AND concept_class_id = 'Module'
            GROUP BY 1
        ")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"primary_consent_date",anchor_date_table,before,after)
    return(result_all)
}

#' Survey from observation table query
#' 
#' @param survey_codes a numeric vector or string of survey codes corresponding to observation concept_id
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, observation_date, survey_obs_question, survey_obs_response
#' @examples 
#' \dontrun{
#' survey_dat <- survey_obs_query(1704039)
#' }
#' @export
survey_obs_query <- function(survey_codes,anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "survey_obs_query.csv"
    survey_codes <- paste0(survey_codes, collapse=",")
    query <- "
    SELECT 
    o.person_id,
    o.observation_date,
    c.concept_name AS survey_obs_question,
    c2.concept_name AS survey_obs_response
    FROM observation o
    INNER JOIN concept c ON (o.observation_source_concept_id = c.concept_id)
    INNER JOIN concept c2 ON (o.value_as_concept_id = c2.concept_id)
    WHERE c.concept_id IN ({survey_codes})
    "
    query <- stringr::str_glue(query)
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"observation_date",anchor_date_table,before,after)
    return(result_all)
}

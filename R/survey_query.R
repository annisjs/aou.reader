#' Survey query
#' 
#' @param survey_codes a character vector or string of survey codes
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, survey_response, survey_date
#' @examples 
#' \dontrun{
#' survey_dat <- survey_query("1585860")
#' }
#' @export
survey_query <- function(survey_codes,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "survey_query.csv"
  survey_codes <- paste0(survey_codes,collapse=",")
  query <- stringr::str_glue("
        SELECT
            survey.person_id,
            survey.answer AS survey_response,
            CAST(survey.survey_datetime AS DATE) AS survey_date
        FROM
            `ds_survey` survey
        WHERE
            (
                question_concept_id IN (
                          {survey_codes}
                )
            )")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"survey_date",anchor_date_table,before,after)
    return(result_all)
}
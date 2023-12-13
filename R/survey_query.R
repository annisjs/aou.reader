#' Survey query
#' @param survey_codes a character vector or string of survey codes
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#' @return 
#' a data.table with the following columns:
#' person_id, survey_response, survey_date
#' @examples 
#' \dontrun{
#' survey_dat <- survey_query("1585860")
#' }
survey_query <- function(survey_codes,page_size=NULL)
{
  survey_codes <- paste0(survey_codes,collapse=",")
  query <- str_glue("
        SELECT
            survey.person_id,
            survey.answer AS survey_response,
            CAST(survey.survey_datetime AS DATE) AS survey_date
        FROM
            `{dataset}.ds_survey` survey
        WHERE
            (
                question_concept_id IN (
                          {survey_codes}
                )
            )")
  download_data(query,page_size)
}
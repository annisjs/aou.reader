#' SES query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, observation_datetime, zip_code, assisted_income, high_school_education, median_income, 
#' no_health_insurance, poverty, vacant_housing, deprivation_index, american_community_survey_year,
#' state_of_residence
#' 
#' @examples 
#' /dontrun{
#' ses_dat <- ses_query()
#' }
#' @export
ses_query <- function(anchor_date_table,before,after)
{
    dest <- "ses_query_result.csv"
    ses_query <- stringr::str_glue("
        SELECT
            observation.person_id,
            observation.observation_datetime,
            cast(observation.observation_datetime AS DATE) AS date,
            zip_code.zip3_as_string as zip_code,
            zip_code.fraction_assisted_income as assisted_income,
            zip_code.fraction_high_school_edu as high_school_education,
            zip_code.median_income,
            zip_code.fraction_no_health_ins as no_health_insurance,
            zip_code.fraction_poverty as poverty,
            zip_code.fraction_vacant_housing as vacant_housing,
            zip_code.deprivation_index,
            zip_code.acs as american_community_survey_year,
            p.state_of_residence_source_value as state_of_residence
        FROM
            `zip3_ses_map` zip_code
        JOIN
            `observation` observation
                ON CAST(SUBSTR(observation.value_as_string,
            0,
            STRPOS(observation.value_as_string,
            '*') - 1) AS INT64) = zip_code.zip3
            AND observation_source_concept_id = 1585250
            AND observation.value_as_string NOT LIKE 'Res%'
        LEFT JOIN person p ON (observation.person_id = p.person_id)")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"date",anchor_date_table,before,after)
    return(result_all)
}
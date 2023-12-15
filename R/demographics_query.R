#' Demographics query#' 
# 
#' @return 
#' a data.table with the following columns:
#' person_id, date_of_birth, race, ethnicity, sex
#' 
#' @examples 
#' /dontrun{
#' dem_dat <- demographics_query()
#' }
#' @export
demographics_query <- function()
{
  dest <- "demographics_query_result.csv"
  query <- stringr::str_glue("
    SELECT
        person.person_id,
        person.birth_datetime as date_of_birth,
        p_race_concept.concept_name as race,
        p_ethnicity_concept.concept_name as ethnicity,
        p_sex_at_birth_concept.concept_name as sex
    FROM
        `person` person
    LEFT JOIN
        `concept` p_race_concept
            ON person.race_concept_id = p_race_concept.concept_id
    LEFT JOIN
        `concept` p_ethnicity_concept
            ON person.ethnicity_concept_id = p_ethnicity_concept.concept_id
    LEFT JOIN
        `concept` p_sex_at_birth_concept
            ON person.sex_at_birth_concept_id = p_sex_at_birth_concept.concept_id", sep="")
  result_all <- download_big_data(query,dest)
  return(result_all)
}
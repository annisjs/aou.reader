
#' Medical encounter query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, measurement_date, bmi
#' 
#' @examples 
#' /dontrun{
#' first_med_enc_dat <- medical_encounter_query()
#' }
#' @export
medical_encounter_query <- function(time="first",anchor_date_table=NULL,before=NULL,after=NULL)
{
  if (time == "first") {
    ordering <- "MIN"
    date_group <- ""
  } else if (time == "last") {
    ordering <- "MAX"
    date_group <- ""
  } else if (time == "count") {
    ordering <- "COUNT"
    date_group <- ""
  } else if (time == "all") {
    ordering <- "MIN"
    date_group <- ",date"
  } else {
    stop("time must be 'first', 'last', 'count' or 'all'")
  }
  dest <- stringr::str_glue("{time}_medical_encounter_query_result.csv")
  if (time == "first" || time == "last"){
    query <- stringr::str_glue("
    WITH ehr AS (
    SELECT person_id, {ordering}(m.measurement_date) AS date
    FROM `measurement` AS m
    LEFT JOIN `measurement_ext` AS mm on m.measurement_id = mm.measurement_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    GROUP BY person_id

    UNION DISTINCT

    SELECT person_id, {ordering}(m.condition_start_date) AS date
    FROM `condition_occurrence` AS m
    LEFT JOIN `condition_occurrence_ext` AS mm on m.condition_occurrence_id = mm.condition_occurrence_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    GROUP BY person_id

    UNION DISTINCT

    SELECT person_id, {ordering}(m.procedure_date) AS date
    FROM `procedure_occurrence` AS m
    LEFT JOIN `procedure_occurrence_ext` AS mm on m.procedure_occurrence_id = mm.procedure_occurrence_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    GROUP BY person_id

    UNION DISTINCT

    SELECT person_id, {ordering}(m.visit_end_date) AS date
    FROM `visit_occurrence` AS m
    LEFT JOIN `visit_occurrence_ext` AS mm on m.visit_occurrence_id = mm.visit_occurrence_id
    WHERE LOWER(mm.src_id) LIKE 'ehr site%'
    GROUP BY person_id

    UNION DISTINCT

    SELECT person_id, {ordering}(m.drug_exposure_start_date) AS date
    FROM `drug_exposure` AS m
    GROUP BY person_id
    )

    SELECT person_id, {ordering}(date) as medical_encounter_entry_date
    FROM ehr
    GROUP BY person_id
    ")} else{
      query <- stringr::str_glue("
            WITH ehr AS (
            SELECT DISTINCT person_id, m.measurement_date AS date
            FROM `measurement` AS m
            LEFT JOIN `measurement_ext` AS mm on m.measurement_id = mm.measurement_id
            WHERE LOWER(mm.src_id) LIKE 'ehr site%'

            UNION DISTINCT

            SELECT DISTINCT person_id, m.condition_start_date AS date
            FROM `condition_occurrence` AS m
            LEFT JOIN `condition_occurrence_ext` AS mm on m.condition_occurrence_id = mm.condition_occurrence_id
            WHERE LOWER(mm.src_id) LIKE 'ehr site%'
            
            UNION DISTINCT

            SELECT DISTINCT person_id, m.procedure_date AS date
            FROM `procedure_occurrence` AS m
            LEFT JOIN `procedure_occurrence_ext` AS mm on m.procedure_occurrence_id = mm.procedure_occurrence_id
            WHERE LOWER(mm.src_id) LIKE 'ehr site%'

            UNION DISTINCT

            SELECT DISTINCT person_id, m.visit_end_date AS date
            FROM `visit_occurrence` AS m
            LEFT JOIN `visit_occurrence_ext` AS mm on m.visit_occurrence_id = mm.visit_occurrence_id
            WHERE LOWER(mm.src_id) LIKE 'ehr site%'
            
            UNION DISTINCT

            SELECT DISTINCT person_id, m.drug_exposure_start_date AS date
            FROM `drug_exposure` AS m
            )
            SELECT person_id, date as medical_encounter_entry_date
            FROM ehr
            ")
    }
  result_all <- download_big_data(query,dest, time != "all")
  if (time == "count"){
    result_all = result_all[, .(count_of_medical_encounter = .N), .(person_id)]
  }
  result_all <- window_data(result_all,"medical_encounter_entry_date",anchor_date_table,before,after)
  return(result_all)
}
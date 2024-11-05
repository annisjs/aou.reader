#' Outpatient Medication query
#' 
#' @param meds a character vector or string containing medication names
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table containing the following columns:
#' person_id, drug_exposure_start_date, drug_type
#' @examples
#' \dontrun{
#' med_dat <- outpatient_med_query(c("roflumilast","daliresp"))
#' }
#' @export
outpatient_med_query <- function(meds,anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "outpatient_med_query_result.csv"
  med_terms <- paste('lower(c.concept_name) LIKE ',"'%",meds,"%'",collapse=' OR ',sep="")
  query <- stringr::str_glue("
       SELECT DISTINCT d.person_id, d.drug_exposure_start_date, c2.concept_name AS drug_type
        FROM
        drug_exposure d
        INNER JOIN
        concept c
        ON (d.drug_concept_id = c.concept_id)
        INNER JOIN
        visit_occurrence v 
        ON (d.visit_occurrence_id = v.visit_occurrence_id)
        INNER JOIN
        concept c2
        ON (d.drug_type_concept_id = c2.concept_id)
        WHERE
        ({med_terms}) AND
        v.visit_concept_id = 9202
    ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"drug_exposure_start_date",anchor_date_table,before,after)
  return(result_all)
}
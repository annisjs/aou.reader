
#' ECG query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#
#' @return 
#' a data.table with the following columns:
#'  "Q-T interval","P-R Interval","T wave axis","R wave axis","P wave axis","QRS duration",
#'  "Q-T interval corrected","QRS complex Ventricles by EKG","P wave Atrium by EKG","Heart rate.beat-to-beat by EKG",
#'  "Q-T interval corrected based on Bazett formula","QRS duration {Electrocardiograph lead}","QRS axis",
#'  "R-R interval by EKG"
#' 
#' @examples
#' \dontrun{
#' ecg_dat <- ecg_query_query()
#' }
#' @export
ecg_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "ecg_query_result.csv"
  ecg_vars <- c("Q-T interval","P-R Interval","T wave axis","R wave axis","P wave axis","QRS duration",
                  "Q-T interval corrected","QRS complex Ventricles by EKG","P wave Atrium by EKG","Heart rate.beat-to-beat by EKG",
                  "Q-T interval corrected based on Bazett formula","QRS duration {Electrocardiograph lead}","QRS axis",
                  "R-R interval by EKG")
  ecg_terms <- paste('c.concept_name LIKE ',"'",ecg_vars,"'",collapse=' OR ',sep="")
  query <- stringr::str_glue("
    SELECT person_id, measurement_date, value_as_number, c.concept_name
    FROM `measurement` m
    INNER JOIN `concept` c ON (m.measurement_concept_id = c.concept_id)
    WHERE
    ({ecg_terms})
  ")
  result_all <- download_big_data(query,dest)
  result_all <- result_all[!duplicated(result_all[,c("person_id","measurement_date","concept_name")])]
  result_all[, concept_name := gsub("[{]|[}]|_by_ekg","",gsub(" |-|[.]","_",tolower(concept_name)))]
  result_all <- result_all[!duplicated(result_all[,c("person_id","measurement_date","concept_name")])]
  result_all <- dcast(result_all, person_id + measurement_date ~ concept_name, value.var = "value_as_number")
  result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
  return(result_all)
}
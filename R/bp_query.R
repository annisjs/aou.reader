#' Blood pressure query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#
#' @return 
#' a data.table with the following columns:
#' person_id, measurement_date, bp_systolic, bp_diastolic
#' 
#' @examples
#' \dontrun{
#' bp_dat <- bp_query()
#' }
#' @export
bp_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  dest <- "bp_query_result.csv"
  query <- stringr::str_glue("
        WITH diatb AS (SELECT
            person_id, measurement_datetime, value_as_number AS bp_diastolic
            FROM `{dataset}.measurement` m
        WHERE
            m.measurement_source_value IN ('8462-4','8453-3', '271650006')),
        systb AS (SELECT
            person_id, measurement_datetime, value_as_number AS bp_systolic
            FROM `{dataset}.measurement` m
        WHERE
            m.measurement_source_value IN ('8480-6','8459-0', '271649006'))
        SELECT d.person_id,
               CAST(d.measurement_datetime AS DATE) AS measurement_date,
               bp_systolic,
               bp_diastolic
        FROM
        diatb d
        INNER JOIN systb s
        ON (d.person_id = s.person_id)
        WHERE
        d.measurement_datetime = s.measurement_datetime")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
  return(result_all)
}
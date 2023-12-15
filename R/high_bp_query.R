#' High Blood Pressure Query
#'
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#'
#' @return
#' A data.table with the following columns:
#' person_id, measurement_date
#' 
#' @description
#' Provides the first date where BP was >=140/>=90
#' 
#' @examples
#' \dontrun{
#' high_bp_dat <- high_bp_query()
#' }
#' 
#' #' @export
high_bp_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
  dest <- "high_bp_query.csv"
  query <- stringr::str_glue("
    WITH diatb AS (SELECT
        person_id, measurement_datetime, value_as_number AS dia
        FROM `measurement` m
    WHERE
        m.measurement_source_value IN ('8462-4','271650006','8453-3')),
    systb AS (SELECT
        person_id, measurement_datetime, value_as_number AS sys
        FROM `measurement` m
    WHERE
        m.measurement_source_value IN ('8480-6','271649006','8459-0'))
    SELECT d.person_id, MIN(CAST(d.measurement_datetime AS DATE)) AS measurement_date
    FROM
    diatb d
    INNER JOIN systb s
    ON (d.person_id = s.person_id)
    WHERE
    d.measurement_datetime = s.measurement_datetime
    AND sys >= 140
    AND dia >= 90
    GROUP BY d.person_id
    ")
  result_all <- download_big_data(query,dest)
  result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
  return(result_all)
}
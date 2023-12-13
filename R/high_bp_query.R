#' High Blood Pressure Query
#'
#' @param page_size The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#'
#' @return
#' A data.table with the following columns:
#' person_id, measurement_date
#' 
#' @description
#' Provides the first date where BP was >=140/>=90
#' 
#' @export
#'
#' @examples
#' \dontrun{
#' high_bp_dat <- high_bp_query()
#' }
high_bp_query <- function(page_size=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  bp_query <- str_glue("
    WITH diatb AS (SELECT
        person_id, measurement_datetime, value_as_number AS dia
        FROM `{dataset}.measurement` m
    WHERE
        m.measurement_source_value IN ('8462-4','271650006','8453-3')),
    systb AS (SELECT
        person_id, measurement_datetime, value_as_number AS sys
        FROM `{dataset}.measurement` m
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
  result <- download_data(bp_query,page_size)
  return(result)
}
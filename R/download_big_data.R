#' Download big data
#' 
#' @param query SQL query string
#' @param dest name of the csv file
#' @param rm_csv logical indicating whether to delete the csv after downloading. Default is TRUE.
#' 
#' @return 
#' If rm_csv is TRUE, it will return a data.table corresponding to the query only.
#' If rm_csv is FALSE, then in addition to the above, a csv file will also be saved to a folder called aou_reader in the workspace bucket.
#' 
#' @examples 
#' \dontrun{
#' dataset <- Sys.getenv("WORKSPACE_CDR")
#' query <- stringr::str_glue("
#'        SELECT
#'            survey.person_id,
#'            survey.answer AS survey_response,
#'            CAST(survey.survey_datetime AS DATE) AS survey_date
#'        FROM
#'            `{dataset}.ds_survey` survey
#'        WHERE
#'            (
#'                question_concept_id IN (
#'                          1586198
#'                )
#'            )")
#' dest <- "survey.csv"
#' res <- download_big_data(query,dest)
#' }
#' 
#' @export
download_big_data <- function(query,dest,rm_csv=TRUE)
{
    bucket <- Sys.getenv("WORKSPACE_BUCKET")
    output_folder <- stringr::str_glue("{bucket}/aou_reader/")
    dest <- paste0(output_folder,gsub(".csv","_*.csv",dest))
    bq_table <- bigrquery::bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), query, billing = Sys.getenv("GOOGLE_PROJECT"))
    bigrquery::bq_table_save(bq_table, dest, destination_format = "CSV")
    res <- data.table::as.data.table(read_bucket(dest))
    if (rm_csv)
    {
        system(stringr::str_glue("gsutil rm {output_folder}*"),intern=TRUE)
    }
    return(res)
}
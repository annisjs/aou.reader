#' Medication query
#' @param meds a character vector or string containing medication names
#' @param page_size = The number of rows requested per chunk. It is recommended to leave this unspecified (see bq_table_download {bigrquery}).
#' @return 
#' a data.table containing the following columns:
#' person_id, drug_exposure_start_date
#' @examples
#' \dontrun{
#' med_dat <- med_query(c("roflumilast","daliresp"))
#' }
#' @export
med_query <- function(meds,page_size=NULL)
{
  dataset <- Sys.getenv("WORKSPACE_CDR")
  med_terms <- paste('lower(c.concept_name) LIKE ',"'%",meds,"%'",collapse=' OR ',sep="")
  query <- str_glue("
       SELECT DISTINCT d.person_id,d.drug_exposure_start_date
        FROM
        {dataset}.drug_exposure d
        INNER JOIN
        {dataset}.concept c
        ON (d.drug_concept_id = c.concept_id)
        WHERE
        {med_terms}
    ")
  download_data(query,page_size)
}
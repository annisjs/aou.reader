#' State of residence query 
# 
#' @return 
#' a data.table with the following columns:
#' person_id, state_of_residence_value
#' 
#' @examples 
#' /dontrun{
#' sor_dat <- state_of_residence_query()
#' }
#' @export
state_of_residence_query <- function()
{
  dest <- "state_of_residence_query_result.csv"
  query <- "
    SELECT
        person_id,
        state_of_residence_source_value AS state_of_residence_value
    FROM
        person_ext
    "
  result_all <- download_big_data(query,dest)
  return(result_all)
}
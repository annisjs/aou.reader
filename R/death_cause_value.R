#' Death cause query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, death_cause_entry_date, death_cause_value
#' 
#' @examples 
#' /dontrun{
#' death_dat <- death_cause_query()
#' }
#' @export
death_cause_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "death_cause_query_result.csv"
    query = str_glue("
        SELECT
            DISTINCT d.person_id
            , d.death_cause_date
            , (SELECT concept_name FROM `concept` WHERE concept_id = cause_concept_id) as death_cause_value
        
        FROM `death` d
        ")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"death_cause_entry_date",anchor_date_table,before,after)
    return(result_all)
}

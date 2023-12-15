
#' Whole genome sequencing cohort query
#'
#' @return 
#' a data.table with the following columns:
#' person_id
#' 
#' @examples
#' \dontrun{
#' wgs_dat <- wgs_query()
#' }
#' @export
wgs_query <- function()
{
    dest <- "wgs_query_result.csv"
    query <- paste("
        SELECT
            person.person_id
        FROM
            `person` person
        WHERE
            person.PERSON_ID IN (
                SELECT
                    distinct person_id
                FROM
                    `cb_search_person` cb_search_person
                WHERE cb_search_person.person_id IN (
                        SELECT
                            person_id
                        FROM
                            `cb_search_person` p
                        WHERE
                            has_whole_genome_variant = 1
                    )
                )", sep="")
    result_all <- download_big_data(query,dest)
    return(result_all)
}
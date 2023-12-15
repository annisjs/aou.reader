#' Deprivation index query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
# 
#' @return 
#' a data.table with the following columns:
#' person_id, deprivation_index_entry_date, deprivation_index_value
#' 
#' @examples 
#' /dontrun{
#' dep_idx <- deprivation_index_query()
#' }
#' @export
deprivation_index_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "deprivation_index_query_result.csv"
    query <- paste("
        SELECT
            observation.person_id,
            observation.observation_date AS deprivation_index_entry_date,
            zip_code.deprivation_index AS deprivation_index_value
        FROM
            `zip3_ses_map` zip_code
        JOIN
            `observation` observation
                ON CAST(SUBSTR(observation.value_as_string,
            0,
            STRPOS(observation.value_as_string,
            '*') - 1) AS INT64) = zip_code.zip3
        WHERE
            observation.PERSON_ID IN (
                SELECT
                    distinct person_id
                FROM
                    `cb_search_person` cb_search_person
                WHERE
                    cb_search_person.person_id IN (
                        SELECT
                            person_id
                        FROM
                            `cb_search_person` p
                        WHERE
                            has_fitbit = 1
                    )
                )
                AND observation_source_concept_id = 1585250
                AND observation.value_as_string NOT LIKE 'Res%'", sep="")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"deprivation_index_entry_date",anchor_date_table,before,after)
    return(result_all)
}
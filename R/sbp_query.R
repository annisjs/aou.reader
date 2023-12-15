#' Systolic blood pressure query
#' 
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, measurement_date, value_as_number 
#' @examples 
#' \dontrun{
#' sbp_dat <- sbp_query()
#' }
#' @export
sbp_query <- function(anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "sbp_query_result.csv"
    query <- stringr::str_glue("
    `SELECT
        measurement.person_id,
        EXTRACT(DATE FROM measurement.measurement_datetime) as measurement_date,
        measurement.value_as_number
    FROM
        ( SELECT
            *
        FROM
            `measurement` measurement
        WHERE
            (
                measurement_concept_id IN  (
                    SELECT
                        DISTINCT c.concept_id
                    FROM
                        `cb_criteria` c
                    JOIN
                        (
                            select
                                cast(cr.id as string) as id
                            FROM
                                `cb_criteria` cr
                            WHERE
                                concept_id IN (
                                    4152194, 3018586, 3004249
                                )
                                AND full_text LIKE '%_rank1]%'
                        ) a
                            ON (
                                c.path LIKE CONCAT('%.',
                            a.id,
                            '.%')
                            OR c.path LIKE CONCAT('%.',
                            a.id)
                            OR c.path LIKE CONCAT(a.id,
                            '.%')
                            OR c.path = a.id)
                        WHERE
                            is_standard = 1
                            AND is_selectable = 1
                        )
                )
            ) measurement")
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,"measurement_date",anchor_date_table,before,after)
    return(result_all)
}
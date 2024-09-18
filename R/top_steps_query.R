#' Top steps query
#' 
#' @param top_number Top x-minute step count, where top_number = x.
#' @param anchor_date_table a data.frame containing two columns: person_id, anchor_date. A time window can be defined around the anchor date using the \code{before} and \code{after} arguments.
#' @param before an integer greater than or equal to 0. Dates prior to anchor_date + before will be excluded.
#' @param after an integer greater than or equal to 0. Dates after anchor_date + after will be excluded.
#' 
#' @return 
#' a data.table with the following columns:
#' person_id, condition_start_date, condition_source_value
#' 
#' @details  Top x-minute step count for each day.
#' 
#' @examples
#' \dontrun{
#' top_steps_dat <- top_steps(1)
#' }
#' @export 
top_steps_query <- function(top_number,anchor_date_table=NULL,before=NULL,after=NULL)
{
    dest <- "top_steps_query.csv"
    if (top_number <= 0) {
        stop("top_number must be greater than 0")
    }
    if (top_number == 1)
    {
        query <- stringr::str_glue("
                SELECT person_id, CAST(datetime AS DATE) AS top1_steps_date, MAX(steps) AS top1_steps_value
                FROM `steps_intraday`
                GROUP BY
                CAST(datetime AS DATE),person_id
            ")
    } else {      
        query <- stringr::str_glue("
            WITH cte AS (
            SELECT  *,
                    ROW_NUMBER() OVER (PARTITION BY person_id, CAST(datetime AS DATE) ORDER BY steps DESC) as rn
            FROM `steps_intraday`
            )
            SELECT person_id, CAST(datetime AS DATE) AS top{top_number}_steps_date, AVG(steps) AS top{top_number}_steps_value
            FROM cte
            WHERE rn <= {top_number}
            GROUP BY
            CAST(datetime AS DATE),person_id
        ")
    }
    result_all <- download_big_data(query,dest)
    result_all <- window_data(result_all,str_glue("top{top_number}_steps_date"),anchor_date_table,before,after)
    return(result_all)
}
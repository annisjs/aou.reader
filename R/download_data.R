#' Download data given query
#'
#' @param query a SQL query string
#' @param page_size The number of rows requested per chunk
#' @return a data.table
#' @import data.table bigrquery
#' @export
download_data <- function(query,page_size=NULL) {
  tb <- bq_project_query(Sys.getenv('GOOGLE_PROJECT'), query)
  as.data.table(bq_table_download(tb,page_size=page_size))
}
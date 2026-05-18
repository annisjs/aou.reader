#' Download big data
#'
#' @param query SQL query string
#' @param dest name of the csv file (e.g., "demographics_query_result.csv")
#' @param rm_csv logical indicating whether to delete the csv after downloading. Default is TRUE.
#'
#' @return
#' A data.table corresponding to the query. If rm_csv is FALSE, files are left in the workspace bucket.
#'
#' @export
download_big_data_robust <- function(query, dest, rm_csv = TRUE) {

  bucket <- Sys.getenv("WORKSPACE_BUCKET")
  if (identical(bucket, "")) stop("WORKSPACE_BUCKET env var is not set.")
  cdr <- Sys.getenv("WORKSPACE_CDR")
  if (identical(cdr, "")) stop("WORKSPACE_CDR env var is not set.")
  billing <- Sys.getenv("GOOGLE_PROJECT")
  if (identical(billing, "")) stop("GOOGLE_PROJECT env var is not set.")

  # Use a folder prefix derived from dest (without .csv) so we can cleanly list/delete outputs
  dest_base <- sub("\\.csv$", "", dest)
  output_folder <- stringr::str_glue("{bucket}/{dest_base}/")

  # Keep the destination free of wildcards. bigrquery may write a single file or multiple shards.
  # Using a stable prefix lets us read either case.
  gcs_prefix <- paste0(output_folder, dest_base)

  bq_table <- bigrquery::bq_dataset_query(cdr, query, billing = billing)
  bigrquery::bq_table_save(bq_table, gcs_prefix, destination_format = "CSV")

  # Read whatever CSVs were produced (single file or sharded files)
  # We list objects in the workspace bucket folder and download matching ones locally.
  local_dir <- file.path(tempdir(), "aou_reader", dest_base)
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)

  # List all objects under the folder
  objs <- system(stringr::str_glue("gsutil ls {output_folder}"), intern = TRUE)

  # Keep only CSVs that match the prefix
  csv_objs <- objs[grepl(paste0("^", gsub("([.^$|()\\[\\]{}*+?\\\\-])", "\\\\\\1", gcs_prefix), ".*\\.csv$"), objs)]
  if (length(csv_objs) == 0) {
    # Fallback: any CSV in the folder
    csv_objs <- objs[grepl("\\.csv$", objs)]
  }
  if (length(csv_objs) == 0) stop("No CSV outputs found in: ", output_folder)

  # Copy down then read+rowbind
  system(stringr::str_glue("gsutil -m cp {output_folder}*.csv {local_dir}/"), intern = TRUE)
  files <- list.files(local_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(files) == 0) stop("CSV copy succeeded but no local files found in: ", local_dir)

  res <- data.table::rbindlist(lapply(files, data.table::fread), fill = TRUE)

  if (rm_csv) {
    # Remove all outputs for this call
    system(stringr::str_glue("gsutil -m rm -r {output_folder}*"), intern = TRUE)
  }

  res
}
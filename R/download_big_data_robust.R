#' Download big data (robust to single-file vs sharded CSV outputs)
#'
#' @param query SQL query string
#' @param dest file name like "demographics_query_result.csv"
#' @param rm_csv logical; delete exported objects from the bucket folder after reading
#'
#' @export
download_big_data_robust <- function(query, dest, rm_csv = TRUE) {

  bucket  <- Sys.getenv("WORKSPACE_BUCKET")
  cdr     <- Sys.getenv("WORKSPACE_CDR")
  billing <- Sys.getenv("GOOGLE_PROJECT")

  if (bucket  == "") stop("WORKSPACE_BUCKET env var is not set.")
  if (cdr     == "") stop("WORKSPACE_CDR env var is not set.")
  if (billing == "") stop("GOOGLE_PROJECT env var is not set.")

  dest_base <- sub("\\.csv$", "", dest)

  # Put outputs in a clean folder not containing ".csv"
  output_folder <- stringr::str_glue("{bucket}/aou_reader/{dest_base}/")

  # Prefix for objects written by bq_table_save (may produce single file or shards)
  # Do NOT include '*' here.
  gcs_prefix <- paste0(output_folder, dest_base)

  bq_table <- bigrquery::bq_dataset_query(cdr, query, billing = billing)
  bigrquery::bq_table_save(bq_table, gcs_prefix, destination_format = "CSV")

  # List objects in the folder
  objs <- system(stringr::str_glue("gsutil ls {output_folder}"), intern = TRUE)

  # Prefer objects that match our prefix; no regex needed
  csv_objs <- objs[startsWith(objs, gcs_prefix) & endsWith(objs, ".csv")]

  # Fallback: any CSV in the folder
  if (length(csv_objs) == 0) {
    csv_objs <- objs[endsWith(objs, ".csv")]
  }
  if (length(csv_objs) == 0) stop("No CSV outputs found in: ", output_folder)

  # Download those objects locally
  local_dir <- file.path(tempdir(), "aou_reader", dest_base)
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)

  # Quote paths in case of odd characters
  for (u in csv_objs) {
    system2("gsutil", c("cp", shQuote(u), shQuote(local_dir)), stdout = TRUE, stderr = TRUE)
  }

  files <- list.files(local_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(files) == 0) stop("No local CSV files found after gsutil cp into: ", local_dir)

  res <- data.table::rbindlist(lapply(files, data.table::fread), fill = TRUE)

  if (rm_csv) {
    system2("gsutil", c("-m", "rm", shQuote(paste0(output_folder, "*"))),
            stdout = TRUE, stderr = TRUE)
  }

  res
}
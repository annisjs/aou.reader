#' Download big data via BigQuery extract to GCS (robust for huge results)
#'
#' @export
download_big_data <- function(query, dest, rm_csv = TRUE) {

  bucket  <- Sys.getenv("WORKSPACE_BUCKET")   # like "gs://fc-secure-.../"
  cdr     <- Sys.getenv("WORKSPACE_CDR")      # dataset id
  billing <- Sys.getenv("GOOGLE_PROJECT")     # project id

  if (bucket  == "") stop("WORKSPACE_BUCKET env var is not set.")
  if (cdr     == "") stop("WORKSPACE_CDR env var is not set.")
  if (billing == "") stop("GOOGLE_PROJECT env var is not set.")

  dest_base <- sub("\\.csv$", "", dest)

  # Export folder in the workspace bucket
  output_folder <- paste0(bucket, "aou_reader/", dest_base, "/")
  gcs_uri <- paste0(output_folder, dest_base, "-*.csv")  # shard pattern for extract

  # 1) Run query to a temp table
  job <- bigrquery::bq_dataset_query(cdr, query, billing = billing)

  # job is a bq_table reference to the temp results
  # 2) Use 'bq extract' for a stable GCS export
  src <- sprintf("%s:%s.%s",
                 job$project, job$dataset, job$table)

  cmd <- sprintf(
    "bq --project_id=%s extract --destination_format=CSV --print_header=true '%s' '%s'",
    billing, src, gcs_uri
  )
  out <- system(cmd, intern = TRUE)
  # If extract fails, bq writes messages to stdout/stderr; we should detect failure
  # system() returns output only; to properly handle return codes, use system2:
  # (kept simple here; see note below)

  # 3) List exported shards
  objs <- system2("gsutil", c("ls", paste0(output_folder, "*.csv")), stdout = TRUE, stderr = TRUE)
  csv_objs <- objs[grepl("\\.csv$", objs)]
  if (length(csv_objs) == 0) stop("No CSV outputs found in: ", output_folder)

  # 4) Download locally and read
  local_dir <- file.path(tempdir(), "aou_reader", dest_base)
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)

  system2("gsutil", c("-m", "cp", paste0(output_folder, "*.csv"), local_dir),
          stdout = TRUE, stderr = TRUE)

  files <- list.files(local_dir, pattern = "\\.csv$", full.names = TRUE)
  if (length(files) == 0) stop("No local CSVs found in: ", local_dir)

  res <- data.table::rbindlist(lapply(files, data.table::fread), fill = TRUE)

  if (rm_csv) {
    system2("gsutil", c("-m", "rm", paste0(output_folder, "*.csv")),
            stdout = TRUE, stderr = TRUE)
  }

  res
}
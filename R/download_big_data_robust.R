#' Download big data via BigQuery extract to GCS (robust for huge results)
#'
#' @export
download_big_data <- function(query, dest, rm_csv = TRUE) {

  bucket  <- Sys.getenv("WORKSPACE_BUCKET")
  cdr     <- Sys.getenv("WORKSPACE_CDR")
  billing <- Sys.getenv("GOOGLE_PROJECT")

  if (bucket  == "") stop("WORKSPACE_BUCKET env var is not set.")
  if (cdr     == "") stop("WORKSPACE_CDR env var is not set.")
  if (billing == "") stop("GOOGLE_PROJECT env var is not set.")

  dest_base <- sub("\\.csv$", "", dest)

  output_folder <- paste0(bucket, "aou_reader/", dest_base, "/")
  # shard pattern for extract (fine to keep)
  gcs_uri <- paste0(output_folder, dest_base, "-*.csv")
  # prefix we expect outputs to start with
  gcs_prefix <- paste0(output_folder, dest_base)

  # 1) Run query to a temp table
  job <- bigrquery::bq_dataset_query(cdr, query, billing = billing)
  src <- sprintf("%s:%s.%s", job$project, job$dataset, job$table)

  # 2) Export via bq extract (capture failure details)
  args <- c(
    paste0("--project_id=", billing),
    "extract",
    "--destination_format=CSV",
    "--print_header=true",
    src,
    gcs_uri
  )
  bq_out <- tempfile("bq_extract_out_")
  bq_err <- tempfile("bq_extract_err_")
  status <- system2("bq", args, stdout = bq_out, stderr = bq_err)

  if (!identical(status, 0L)) {
    stop(
      "bq extract failed (exit code ", status, ")\n",
      "STDOUT:\n", paste(readLines(bq_out, warn = FALSE), collapse = "\n"), "\n\n",
      "STDERR:\n", paste(readLines(bq_err, warn = FALSE), collapse = "\n")
    )
  }

  # 3) List exported outputs (don’t assume .csv)
  objs <- system2("gsutil", c("ls", output_folder), stdout = TRUE, stderr = TRUE)
  objs <- objs[nzchar(objs)]

  # Prefer CSV shards if present
  out_objs <- objs[startsWith(objs, gcs_prefix) & endsWith(objs, ".csv")]

  # Fallback: accept objects with no extension (e.g., .../dest_base)
  if (length(out_objs) == 0) {
    out_objs <- objs[startsWith(objs, gcs_prefix)]
  }

  if (length(out_objs) == 0) stop("No outputs found in: ", output_folder)

  # 4) Download locally and read
  local_dir <- file.path(tempdir(), "aou_reader", dest_base)
  dir.create(local_dir, recursive = TRUE, showWarnings = FALSE)

  for (u in out_objs) {
    system2("gsutil", c("cp", u, local_dir), stdout = TRUE, stderr = TRUE)
  }

  files <- list.files(local_dir, full.names = TRUE)
  if (length(files) == 0) stop("No local files found in: ", local_dir)

  res <- data.table::rbindlist(lapply(files, data.table::fread), fill = TRUE)

  if (rm_csv) {
    # remove everything produced for this dest_base (csv shards + extensionless)
    system2("gsutil", c("-m", "rm", paste0(output_folder, "*")),
            stdout = TRUE, stderr = TRUE)
  }

  res
}
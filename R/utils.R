#' Open a read-only connection to the database
#'
#' @param db_path Path to the DuckDB database file. Defaults to `DB_PATH` env var.
#' @return A DBI connection object
#' @export
connect <- function(db_path = Sys.getenv("DB_PATH")) {
  connections::connection_open(
    duckdb::duckdb(),
    db_path,
    read_only = TRUE
  )
}

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

# connect to the db
#' @export
tblx <- function(schema, table) {
  if (!exists("con", envir = .GlobalEnv)) {
    stop(
      "Database connection (`con`) not found in global environment.
          Please create a connection before using this function."
    )
  }

  schema <- rlang::as_string(rlang::ensym(schema))
  table <- rlang::as_string(rlang::ensym(table))

  schema <- stringr::str_to_upper(schema)
  table <- stringr::str_to_upper(table)
  dplyr::tbl(
    get("con", envir = .GlobalEnv),
    DBI::Id(schema = schema, table = table)
  )
}

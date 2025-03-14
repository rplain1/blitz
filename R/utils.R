#' connect to this db yo
#'
#' @export
connect <- function() {
  DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH"))
}

#' @export
tblx <- function(schema, table) {
  if (!exists("con", envir = .GlobalEnv)) {
    stop(
      "Database connection (`con`) not found in global environment.
          Please create a connection before using this function."
    )
  }
  schema <- stringr::str_to_upper(schema)
  table <- stringr::str_to_upper(table)
  dplyr::tbl(
    get("db_connection", envir = .GlobalEnv),
    DBI::Id(schema = schema, table = table)
  )
}

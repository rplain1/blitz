# Load necessary libraries
library(DBI)
library(duckdb)
library(RSQLite)
library(arrow)

update_nflfastR_db <- function(dbdir = '~/.db', force_rebuild = F) {
  nflfastR::update_db(dbdir = dbdir, force_rebuild = force_rebuild)
}
# Constructor
new_etl <- function(config_file) {
  # Initialize object
  etl <- list(
    config = read_config(config_file),
    raw_data = NULL,
    transformed_data = NULL
  )
  class(etl) <- "etl"

  return(etl)
}

# Helper functions
read_config <- function(config_file) {
  cfg <- config::get(file = config_file)
  return(cfg)
}

# setup_logging <- function() {
#   logger::log_threshold(logger::INFO)
#   logger::log_layout(logger::layout_glue_generator(
#     format = "{timestamp} - {level} - {msg}"
#   ))
# }

# Methods
extract <- function(etl_obj) {
  UseMethod("extract")
}

extract.etl <- function(etl_obj) {
  tryCatch(
    {
      sqlite_path <- etl_obj$config$database$sqlite$path
      table <- etl_obj$config$database$sqlite$table

      con <- DBI::dbConnect(RSQLite::SQLite(), sqlite_path)
      on.exit(DBI::dbDisconnect(con), add = TRUE)

      etl_obj$raw_data <- DBI::dbGetQuery(
        con,
        glue::glue_sql("SELECT * FROM {`table`}", .con = con)
      )
      log_info(glue::glue("{nrow(etl_obj$raw_data)} rows extracted"))
      return(etl_obj)
    },
    error = function(e) {
      return(etl_obj)
    }
  )
}

transform <- function(etl_obj) {
  UseMethod("transform")
}

transform.etl <- function(etl_obj) {
  tryCatch(
    {
      # Add timestamp column
      etl_obj$transformed_data <- etl_obj$raw_data
      etl_obj$transformed_data$RUN_AT <- Sys.time()
      log_info("data transformed")
      return(etl_obj)
    },
    error = function(e) {
      return(etl_obj)
    }
  )
}

load_data <- function(etl_obj) {
  UseMethod("load_data")
}

load_data.etl <- function(etl_obj) {
  tryCatch(
    {
      duckdb_path <- etl_obj$config$database$duckdb$path
      schema <- etl_obj$config$database$duckdb$schema
      table <- etl_obj$config$database$duckdb$table

      con <- DBI::dbConnect(duckdb::duckdb(), duckdb_path)
      on.exit(DBI::dbDisconnect(con), add = TRUE)

      # Create schema if it doesn't exist
      DBI::dbExecute(
        con,
        glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema`}", .con = con)
      )

      # Drop table if it exists
      DBI::dbExecute(
        con,
        glue::glue_sql("DROP TABLE IF EXISTS {`schema`}{`table`}", .con = con)
      )

      # Create table from data
      DBI::dbWriteTable(
        con,
        DBI::Id(schema = schema, table = table),
        etl_obj$transformed_data
      )
      log_info(glue::glue("data loaded into {schema}.{table}"))
      return(etl_obj)
    },
    error = function(e) {
      return(etl_obj)
    }
  )
}

# Main execution function
run <- function(etl_obj) {
  UseMethod("run")
}

run.etl <- function(etl_obj) {
  etl_obj <- extract(etl_obj)
  etl_obj <- transform(etl_obj)
  etl_obj <- load_data(etl_obj)
  return(etl_obj)
}

# Example usage
#etl <- new_etl("config.yml")
#etl <- run(etl)

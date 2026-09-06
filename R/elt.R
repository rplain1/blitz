# Suppress R CMD check notes for NSE column names
utils::globalVariables(c("cols", "nflverse_game_id", "updated_at"))

#' Update the nflfastR Database
#'
#' This function is a wrapper that updates the `nflfastR` database by
#' calling `nflfastR::update_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `luna`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param tblname name or DBI::Id(schema, table) of the table location
#' @param db_path Character. Path to the DuckDB database file. Defaults to `DB_PATH` env var.
#'
#' @return No return value. Updates the nflfastR database in place.
#' @export
#'
#' @examples
#' \dontrun{
#' update_pbp()  # Updates the database in the default location
#' update_pbp(force_rebuild = TRUE)  # Forces a rebuild
#' }
update_pbp <- function(
  dbdir = '~/.db',
  dbname = 'luna',
  force_rebuild = FALSE,
  tblname = "NFLFASTR_PBP",
  db_path = Sys.getenv("DB_PATH")
) {
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  nflfastR::update_db(
    dbdir = dbdir,
    dbname = dbname,
    tblname = tblname,
    force_rebuild = force_rebuild,
    db_connection = con
  )
}

#' Align a database table's schema with an incoming data frame
#'
#' Internal helper that adds any columns present in `df` but missing from
#' the target table (e.g. new columns added upstream by nflreadr), filling
#' existing rows with `NULL` for the new columns. This lets season-scoped
#' incremental loads keep working after an upstream base table schema
#' change, without requiring a full table rebuild.
#'
#' @param con A DBI connection
#' @param schema_name name of schema
#' @param table_name name of table
#' @param df incoming data frame
#'
#' @return Invisible NULL
#' @keywords internal
.align_table_schema <- function(con, schema_name, table_name, df) {
  existing_cols <- DBI::dbListFields(
    con,
    DBI::Id(schema = schema_name, table = table_name)
  )
  new_cols <- setdiff(names(df), existing_cols)

  if (length(new_cols) > 0) {
    cli::cli_alert_warning(
      "Upstream schema change detected: adding {length(new_cols)} new column{?s} to {.field {schema_name}.{table_name}} ({.field {new_cols}})"
    )

    for (col in new_cols) {
      col_type <- DBI::dbDataType(con, df[[col]])
      DBI::dbExecute(
        con,
        glue::glue_sql(
          "ALTER TABLE {`schema_name`}.{`table_name`} ADD COLUMN {`col`} {DBI::SQL(col_type)}",
          .con = con
        )
      )
    }
  }

  invisible(NULL)
}

#' Update the specified database table
#'
#' this function will load data into the db. It can handle creating new schemas
#' and tables, and will overwrite data if there is a specified season or set of seasons.
#'
#' @param df A data frame to load into the db
#' @param table_name name of table to use in db
#' @param schema_name name of schema to use, defaults to `"BASE"``
#' @param db_path location of db
#' @param seasons seasons to update when using existing database.
#'
#' @return No return value. Updates the specified db
#' @export
load_data <- function(
  df,
  table_name,
  schema_name = "main",
  db_path = Sys.getenv("DB_PATH"),
  seasons = NULL
) {

  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)


  df <- df |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = updated_at - lubridate::hours(6)
    )

  DBI::dbExecute(
    con,
    glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema_name`}", .con = con)
  )

  full_table_name <- paste0(schema_name, ".", table_name)

  if (!is.numeric(seasons)) {
    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = df,
      overwrite = TRUE
    )
  } else {
    seasons_label <- if (length(seasons) == 1) {
      as.character(seasons)
    } else {
      paste0(min(seasons), "-", max(seasons))
    }

    cli::cli_alert_info(
      "Updating {.field {full_table_name}} for season{?s} {.val {seasons_label}} ({.val {nrow(df)}} records)"
    )

    if (DBI::dbExistsTable(con, DBI::Id(schema_name, table_name))) {
      .align_table_schema(con, schema_name, table_name, df)

      record_check <- DBI::dbGetQuery(
        con,
        glue::glue_sql(
          "SELECT COUNT(*) FROM {`schema_name`}.{`table_name`} WHERE SEASON IN ({vals*})",
          vals = seasons,
          .con = con
        )
      )

      if (record_check[1, 1] > 0) {
        cli::cli_alert_warning(
          "Dropping {.val {record_check[1, 1]}} existing record{?s} for season{?s} {.val {seasons_label}}"
        )

        DBI::dbExecute(
          con,
          glue::glue_sql(
            "DELETE FROM {`schema_name`}.{`table_name`} WHERE SEASON IN ({vals*})",
            vals = seasons,
            .con = con
          )
        )
      }
    }

    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = df,
      overwrite = FALSE,
      append = TRUE
    )
  }

  cli::cli_alert_success(
    "Loaded {.val {nrow(df)}} record{?s} into {.field {full_table_name}}"
  )
}

#' Safely load and store nflreadr data
#'
#' Internal helper that wraps a load expression in error handling.
#' On success, loads data to the database. On failure, logs and continues.
#'
#' @param expr Expression to evaluate (should return a data frame)
#' @param table_name Name of the target table
#' @param seasons Seasons parameter for load_data
#' @param results Environment to track success/failure counts
#'
#' @return Invisible NULL
#' @keywords internal
.safe_load <- function(expr, table_name, seasons = NULL, results) {

  tryCatch(
    {
      cli::cli_progress_step("Loading {.field {table_name}}")
      df <- expr
      load_data(df, table_name = table_name, seasons = seasons)
      results$success <- c(results$success, table_name)
      cli::cli_alert_success("{.field {table_name}} loaded successfully")
    },
    error = function(e) {
      results$failed <- c(results$failed, table_name)
      cli::cli_alert_danger(
        "Failed to load {.field {table_name}}: {conditionMessage(e)}"
      )
    }
  )
  invisible(NULL)
}

#' Update nflreadr tables in the database
#'
#' This function will update tables based on the ENV variable `DB_PATH` that is used in
#' `load_data()`. Each table load is attempted independently - if one fails, the function
#' logs the error and continues with the remaining tables.
#'
#' @param seasons numeric vector to indentify seasons to back filter for and backfill.
#' Note: this should typically only be used with `nflfastr::most_recent_season()` or maintain
#' the default value of `NULL`, as datasets have different years that started collecting. It is
#' easier to do a full refresh than handle all the different conditions.
#'
#' @return Invisibly returns a list with `success` and `failed` table names
#' @export
update_nflreadr_db <- function(seasons = NULL) {
  results <- new.env(parent = emptyenv())
  results$success <- character()
  results$failed <- character()

  cli::cli_h1("Updating nflreadr database tables")


  .safe_load(
    nflreadr::load_teams(),
    table_name = "TEAMS",
    results = results
  )

  .safe_load(
    nflreadr::load_players(),
    table_name = "PLAYERS",
    results = results
  )

  .safe_load(
    nflreadr::load_combine(seasons = seasons),
    table_name = "COMBINE",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_draft_picks(seasons = seasons),
    table_name = "DRAFT_PICKS",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_contracts() |> dplyr::select(-cols),
    table_name = "CONTRACTS",
    results = results
  )

  .safe_load(
    nflreadr::load_espn_qbr(
      seasons = seasons[seasons >= 2006],
      summary_type = "season"
    ),
    table_name = "QBR_SEASON",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_espn_qbr(
      seasons = seasons[seasons >= 2006],
      summary_type = "week"
    ),
    table_name = "QBR_WEEK",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_ff_playerids(),
    table_name = "FF_PLAYERIDS",
    results = results
  )

  .safe_load(
    nflreadr::load_ftn_charting(seasons = seasons[seasons >= 2022]),
    table_name = "FTN_CHARTING",
    seasons = seasons,
    results = results
  )

  if (is.null(seasons) || all(seasons != nflreadr::most_recent_season())) {
    participation_seasons <- if (is.null(seasons)) {
      NULL
    } else {
      seasons[seasons >= 2016 & seasons < nflreadr::most_recent_season()]
    }
    .safe_load(
      nflreadr::load_participation(seasons = participation_seasons) |>
        dplyr::mutate(season = as.numeric(substr(nflverse_game_id, 1, 4))),
      table_name = "PARTICIPATION",
      seasons = seasons,
      results = results
    )
  }

  .safe_load(
    nflreadr::load_nextgen_stats(
      stat_type = "passing",
      seasons = seasons[seasons >= 2016]
    ),
    table_name = "NGS_PASSING",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_nextgen_stats(
      seasons = seasons[seasons >= 2016],
      stat_type = "receiving"
    ),
    table_name = "NGS_RECEIVING",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_nextgen_stats(
      seasons = seasons[seasons >= 2016],
      stat_type = "rushing"
    ),
    table_name = "NGS_RUSHING",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_ff_opportunity(
      seasons = seasons[seasons >= 2016],
      stat_type = "weekly"
    ),
    table_name = "FF_OPPURTUNITY_WK",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_ff_opportunity(
      seasons = seasons[seasons >= 2016],
      stat_type = "pbp_pass"
    ),
    table_name = "FF_OPPURTUNITY_PASS",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_ff_opportunity(
      seasons = seasons[seasons >= 2016],
      stat_type = "pbp_rush"
    ),
    table_name = "FF_OPPURTUNITY_RUSH",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_officials(seasons = seasons[seasons >= 2015]),
    table_name = "OFFICIALS",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_injuries(seasons = seasons[seasons >= 2009]),
    table_name = "INJURIES",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_schedules(seasons = seasons),
    table_name = "SCHEDULES",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_trades(seasons = seasons),
    table_name = "TRADES",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_snap_counts(seasons = seasons[seasons >= 2012]),
    table_name = "SNAP_COUNTS",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_pfr_advstats(
      seasons = seasons[seasons >= 2018],
      stat_type = "pass",
      summary_level = "season"
    ),
    table_name = "PFR_ADV_PASS",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_pfr_advstats(
      seasons = seasons[seasons >= 2018],
      stat_type = "pass",
      summary_level = "season"
    ),
    table_name = "PFR_ADV_PASS_SEASON",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_pfr_advstats(
      seasons = seasons[seasons >= 2018],
      stat_type = "rush",
      summary_level = "season"
    ),
    table_name = "PFR_ADV_RUSH_SEASON",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_pfr_advstats(
      seasons = seasons[seasons >= 2018],
      stat_type = "rec",
      summary_level = "season"
    ),
    table_name = "PFR_ADV_REC_SEASON",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_depth_charts(seasons = seasons[seasons >= 2001]),
    table_name = "DEPTH_CHARTS",
    seasons = seasons,
    results = results
  )

  # Summary

  cli::cli_h2("Summary")
  cli::cli_alert_info(
    "{length(results$success)} table{?s} loaded successfully"
  )
  if (length(results$failed) > 0) {
    cli::cli_alert_warning(
      "{length(results$failed)} table{?s} failed: {.field {results$failed}}"
    )
  }

  invisible(list(
    success = results$success,
    failed = results$failed
  ))
}

#' Update tables related to `nflreadr::calculate_stats()`
#'
#' This function will update tables that are derived from `nflreadr::calculate_stats()`
#' which updated breaking changes in version 1.5. Each table load is attempted
#' independently - if one fails, the function logs the error and continues.
#'
#' @param seasons numeric vector of seasons to specify.
#'
#' @return Invisibly returns a list with `success` and `failed` table names
#' @export
update_team_player_stats <- function(seasons) {
  results <- new.env(parent = emptyenv())
  results$success <- character()
  results$failed <- character()

  cli::cli_h1("Updating team/player stats tables")

  .safe_load(
    nflreadr::load_team_stats(
      seasons = seasons,
      summary_level = "week"
    ),
    table_name = "TEAM_STATS_WK",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_team_stats(
      seasons = seasons,
      summary_level = "reg"
    ),
    table_name = "TEAM_STATS_SEASON",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_player_stats(
      seasons = seasons,
      summary_level = "week"
    ),
    table_name = "PLAYER_STATS_WK",
    seasons = seasons,
    results = results
  )

  .safe_load(
    nflreadr::load_player_stats(
      seasons = seasons,
      summary_level = "reg"
    ),
    table_name = "PLAYER_STATS_SEASON",
    seasons = seasons,
    results = results
  )

  # Summary
  cli::cli_h2("Summary")
  cli::cli_alert_info(
    "{length(results$success)} table{?s} loaded successfully"
  )
  if (length(results$failed) > 0) {
    cli::cli_alert_warning(
      "{length(results$failed)} table{?s} failed: {.field {results$failed}}"
    )
  }

  invisible(list(
    success = results$success,
    failed = results$failed
  ))
}

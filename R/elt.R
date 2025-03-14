#' Update the nflfastR Database
#'
#' This function is a wrapper that updates the `nflfastR` database by
#' calling `nflfastR::update_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `luna`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param tblname name or DBI::Id(schema, table) of the table location
#' @param db_connection A `DBI::dbConnect()` object
#'
#' @return No return value. Updates the nflfastR database in place.
#' @export
#'
#' @examples
#' \dontrun{
#' update_nflfastR_db()  # Updates the database in the default location
#' update_nflfastR_db(dbdir = "data/nfl", force_rebuild = TRUE)  # Forces a rebuild
#' }
update_pbp <- function(
  dbdir = '~/.db',
  dbname = 'luna',
  force_rebuild = FALSE,
  tblname = DBI::Id(schema = "BASE", table = 'NFLFASTR_PBP'),
  db_connection = DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH"))
) {
  nflfastR::update_db(
    dbdir = dbdir,
    dbname = dbname,
    tblname = tblname,
    force_rebuild = force_rebuild,
    db_connection = db_connection
  )
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
  schema_name = "BASE",
  db_path = Sys.getenv("DB_PATH"),
  seasons = NULL
) {
  # initialize db connection
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # add metadata to database load time
  df <- df |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = updated_at - lubridate::hours(6)
    )

  # Create schema if it doesn't exist
  DBI::dbExecute(
    con,
    glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema_name`}", .con = con)
  )

  if (!is.numeric(seasons)) {
    # overwrite all the existing data
    DBI::dbWriteTable(
      con = con,
      name = DBI::Id(schema = schema_name, table = table_name),
      value = df,
      overwrite = TRUE
    )
  } else {
    # backfill or modify select values
    message(glue::glue(
      '{format(Sys.time(), "%H:%M:%S")} | `seasons` provided set to {seasons}, updating {nrow(df)} records'
    ))

    if (DBI::dbExistsTable(con, DBI::Id(schema_name, table_name))) {
      # check that table exists
      record_check <- DBI::dbGetQuery(
        con,
        glue::glue_sql(
          "SELECT COUNT(*) FROM {`schema_name`}.{`table_name`} WHERE SEASON IN ({vals*})",
          vals = seasons,
          .con = con
        )
      )
      if (record_check[1, 1] > 0) {
        # if data exists

        # TODO: look into why this outputs messages for multiple years funky
        # `seasons` provided set to 2017, updating 192340 records14:00:55 | `seasons` provided set to 2018, updating 192340 records14:00:55 | `seasons` provided set to 2019, updating 192340 records14:00:55 | `seasons` provided set to 2020, updating 192340 records
        message(glue::glue(
          '{format(Sys.time(), "%H:%M:%S")} | records exist for `seasons`: {seasons}, dropping existing records records'
        ))

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

  message(glue::glue(
    '{format(Sys.time(), "%H:%M:%S")} | {nrow(df)} records loaded into {schema_name}.{table_name}'
  ))
}

#' main function to update tables that are related to `{nflreadr}`.
#'
#' This function will update tables based on the ENV variable `DB_PATH` that is used in
#' `load_data()`.
#'
#' @param seasons numeric vector to indentify seasons to back filter for and backfill.
#' Note: this should typically only be used with `nflfastr::most_recent_season()` or maintain
#' the default value of `NULL`, as datasets have different years that started collecting. It is
#' easier to do a full refresh than handle all the different conditions.
#'
#' @export
update_nflreadr_db <- function(seasons = NULL) {
  nflreadr::load_teams() |> load_data(table_name = "TEAMS")

  nflreadr::load_combine() |>
    load_data(table_name = "COMBINE", seasons = seasons)

  nflreadr::load_draft_picks() |>
    load_data(table_name = "DRAFT_PICKS", seasons = seasons)

  nflreadr::load_contracts() |>
    dplyr::select(-cols) |>
    load_data(table_name = "CONTRACTS")

  nflreadr::load_espn_qbr(summary_type = "season") |>
    load_data(table_name = "QBR_SEASON", seasons = seasons)

  nflreadr::load_espn_qbr(summary_type = "week") |>
    load_data(table_name = "QBR_WEEK", seasons = seasons)

  nflreadr::load_ff_playerids() |>
    load_data(table_name = "FF_PLAYERIDS")

  nflreadr::load_ftn_charting() |>
    load_data(table_name = "FTN_CHARTING", seasons = seasons)

  nflreadr::load_nextgen_stats(stat_type = 'passing') |>
    load_data(table_name = "NGS_PASSING", seasons = seasons)

  nflreadr::load_nextgen_stats(stat_type = 'receiving') |>
    load_data(table_name = "NGS_RECEIVING", seasons = seasons)

  nflreadr::load_nextgen_stats(stat_type = 'rushing') |>
    load_data(table_name = "NGS_RUSHING", seasons = seasons)

  nflreadr::load_ff_opportunity(stat_type = 'weekly') |>
    load_data(table_name = 'FF_OPPURTUNITY_WK', seasons = seasons)

  nflreadr::load_ff_opportunity(stat_type = 'pbp_pass') |>
    load_data(table_name = 'FF_OPPURTUNITY_PASS', seasons = seasons)

  nflreadr::load_ff_opportunity(stat_type = 'pbp_rush') |>
    load_data(table_name = 'FF_OPPURTUNITY_RUSH', seasons = seasons)

  nflreadr::load_officials() |>
    load_data(table_name = "OFFICIALS")

  nflreadr::load_injuries() |>
    load_data(table_name = 'INJURIES', seasons = seasons)

  nflreadr::load_schedules() |>
    load_data(table_name = "SCHEDULES", seasons = seasons)

  nflreadr::load_trades() |> load_data(table_name = "TRADES", seasons = seasons)

  nflreadr::load_snap_counts() |>
    load_data(table_name = "SNAP_COUNTS", seasons = seasons)

  nflreadr::load_players() |> load_data(table_name = "PLAYERS")

  nflreadr::load_pfr_advstats(stat_type = 'pass', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_PASS', seasons = seasons)

  nflreadr::load_pfr_advstats(stat_type = 'pass', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_PASS_SEASON', seasons = seasons)

  nflreadr::load_pfr_advstats(stat_type = 'rush', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_RUSH_SEASON', seasons = seasons)

  nflreadr::load_pfr_advstats(stat_type = 'rec', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_REC_SEASON', seasons = seasons)
}

#' Update tables related to `nflfastR::calculate_stats()`
#'
#' This function will update tables that are derived from `nflfastR::calculate_stast()`.
#'
#' @param seasons numeric vector of seasons to specifiy.
update_nflfastr_stats <- function(seasons) {
  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "week",
    stat_type = "team"
  ) |>
    load_data(table_name = "TEAM_STATS_WK", seasons = seasons)

  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "season",
    stat_type = "team"
  ) |>
    load_data(table_name = "TEAM_STATS_SEASON", seasons = seasons)

  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "week",
    stat_type = "player"
  ) |>
    load_data(table_name = "PLAYER_STATS_WK", seasons = seasons)

  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "season",
    stat_type = "player"
  ) |>
    load_data(table_name = "PLAYER_STATS_SEASON", seasons = seasons)
}

#' Update the nflfastR Database
#'
#' This function is a wrapper that updates the `nflfastR` database by
#' calling `nflfastR::update_db()`.
#'
#' @param dbdir Character. The directory where the database is stored. Defaults to `~/.db`.
#' @param dbname Character. DB name, defaults to my dog `luna`
#' @param force_rebuild Logical. If `TRUE`, forces a rebuild of the database. Defaults to `FALSE`.
#' @param tblname S4 object or charchter. DBI::Id(schema, table) of the table location
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

load_data <- function(
  .data,
  table_name,
  schema_name = "BASE",
  db_path = Sys.getenv("DB_PATH"),
  seasons = NULL
) {
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  # add metadata to database load time
  .data <- .data |>
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
      value = .data,
      overwrite = TRUE
    )
  } else {
    # backfill or modify select values
    message(glue::glue(
      '{format(Sys.time(), "%H:%M:%S")} | `seasons` provided set to {seasons}, updating {nrow(.data)} records'
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
      value = .data,
      overwrite = FALSE,
      append = TRUE
    )
  }

  message(glue::glue(
    '{format(Sys.time(), "%H:%M:%S")} | {nrow(.data)} records loaded into {schema_name}.{table_name}'
  ))
}


update_nflreadr_db <- function(seasons) {
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

update_nflfastR_db <- function(dbdir = '~/.db', force_rebuild = F) {
  nflfastR::update_db(dbdir = dbdir, force_rebuild = force_rebuild)
}


extract_pbp <- function(cfg, force_rebuild = TRUE) {
  tryCatch(
    {
      sqlite_path <- cfg$database$sqlite$path
      table <- cfg$database$sqlite$table

      con <- DBI::dbConnect(RSQLite::SQLite(), sqlite_path)
      on.exit(DBI::dbDisconnect(con), add = TRUE)

      raw_data <- DBI::dbGetQuery(
        con,
        glue::glue_sql("SELECT * FROM {`table`};", .con = con)
      )
      message(glue::glue(
        '{format(Sys.time(), "%Y-%m-%d %H:%M:%S")} --- INFO --- {nrow(raw_data)} records extracted from {table}'
      ))
      return(raw_data)
    },
    error = function(e) {
      return(e)
    }
  )
}

transform_data <- function(.data) {
  .data |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = updated_at - lubridate::hours(6)
    )
}

load_data <- function(
  .data,
  table_name,
  schema_name = "BASE",
  db_path = Sys.getenv("DB_PATH")
) {
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  # Create schema if it doesn't exist
  DBI::dbExecute(
    con,
    glue::glue_sql("CREATE SCHEMA IF NOT EXISTS {`schema_name`}", .con = con)
  )
  DBI::dbWriteTable(
    con = con,
    name = DBI::Id(schema = schema_name, table = table_name),
    value = .data,
    overwrite = TRUE
  )
  message(glue::glue(
    '{format(Sys.time(), "%Y-%m-%d %H:%M:%S")} --- INFO --- {nrow(.data)} records loaded into {schema_name}.{table_name}'
  ))
}


update_nflpbp_duckdb <- function(cfg = config::get(file = 'config.yml')) {
  extract_pbp(cfg) |>
    transform_data() |>
    load_data(table_name = 'NFLFASTR_PBP')
}

update_nflpbp_duckdb(cfg)

update_nflreadr_db <- function() {
  nflreadr::load_teams() |> load_data(table_name = "TEAMS")

  nflreadr::load_teams() |> load_data(table_name = "TEAMS")

  nflreadr::load_combine() |> load_data(table_name = "COMBINE")

  nflreadr::load_draft_picks() |> load_data(table_name = "DRAFT_PICKS")

  nflreadr::load_contracts() |>
    dplyr::select(-cols) |>
    load_data(table_name = "CONTRACTS")

  nflreadr::load_espn_qbr(summary_type = "season") |>
    load_data(table_name = "QBR_SEASON")

  nflreadr::load_espn_qbr(summary_type = "week") |>
    load_data(table_name = "QBR_WEEK")

  nflreadr::load_ff_playerids() |> load_data(table_name = "FF_PLAYERIDS")

  nflreadr::load_ftn_charting() |> load_data(table_name = "FTN_CHARTING")

  nflreadr::load_nextgen_stats(stat_type = 'passing') |>
    load_data(table_name = "NGS_PASSING")

  nflreadr::load_nextgen_stats(stat_type = 'receiving') |>
    load_data(table_name = "NGS_RECEIVING")

  nflreadr::load_nextgen_stats(stat_type = 'rushing') |>
    load_data(table_name = "NGS_RUSHING")

  nflreadr::load_ff_opportunity(stat_type = 'weekly') |>
    load_data(table_name = 'FF_OPPURTUNITY_WK')

  nflreadr::load_ff_opportunity(stat_type = 'pbp_pass') |>
    load_data(table_name = 'FF_OPPURTUNITY_PASS')

  nflreadr::load_ff_opportunity(stat_type = 'pbp_rush') |>
    load_data(table_name = 'FF_OPPURTUNITY_RUSH')

  nflreadr::load_officials() |> load_data(table_name = "OFFICIALS")

  nflreadr::load_injuries() |> load_data(table_name = 'INJURIES')

  nflreadr::load_schedules() |> load_data(table_name = "SCHEDULES")

  nflreadr::load_trades() |> load_data(table_name = "TRADES")

  nflreadr::load_snap_counts() |> load_data(table_name = "SNAP_COUNTS")

  nflreadr::load_players() |> load_data(table_name = "PLAYERS")

  nflreadr::load_pfr_advstats(stat_type = 'pass', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_PASS')

  nflreadr::load_pfr_advstats(stat_type = 'pass', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_PASS_SEASON')

  nflreadr::load_pfr_advstats(stat_type = 'rush', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_RUSH_SEASON')

  nflreadr::load_pfr_advstats(stat_type = 'rec', summary_level = 'season') |>
    load_data(table_name = 'PFR_ADV_REC_SEASON')
}

#TODO: this would benefit from WHERE conditions to do all years
update_nflfastr_stats <- function() {
  seasons = 2023:2024
  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "week",
    stat_type = "team"
  ) |>
    load_data(table_name = "TEAM_STATS_WK")

  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "season",
    stat_type = "team"
  ) |>
    load_data(table_name = "TEAM_STATS_SEASON")

  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "week",
    stat_type = "player"
  ) |>
    load_data(table_name = "PLAYER_STATS_WK")

  nflfastR::calculate_stats(
    seasons = seasons,
    summary_level = "season",
    stat_type = "player"
  ) |>
    load_data(table_name = "PLAYER_STATS_SEASON")
}

df_teams <- nflreadr::load_teams() |>
  tibble::as_tibble()

con <- DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH"))


transform_data <- function(.data) {
  .data |>
    tibble::as_tibble() |>
    dplyr::mutate(
      updated_at = Sys.time(),
      local_updated_at = updated_at - lubridate::hours(6)
    )
}

load_data <- function(con, .data, table_name, schema_name = "BASE") {
  DBI::dbWriteTable(
    con = con,
    name = DBI::Id(schema = schema_name, table = table_name),
    value = .data,
    overwrite = TRUE
  )
}


run_etl <- function(con, .data, table_name, schema_name = "BASE") {
  .data = transform_data(.data)
  load_data(
    con,
    .data,
    schema_name = schema_name,
    table_name = table_name
  )
  log_info(glue::glue(
    "{nrow(.data)} records written to {schema_name}.{table_name}"
  ))
}

run_etl(con, nflreadr::load_teams(), "TEAMS")
run_etl(con, nflreadr::load_combine(), "COMBINE")
run_etl(con, nflreadr::load_draft_picks(), "DRAFT_PICKS")
run_etl(con, nflreadr::load_contracts() |> dplyr::select(-cols), "CONTRACTS")
run_etl(con, nflreadr::load_espn_qbr(summary_type = "season"), "QBR_SEASON")
run_etl(con, nflreadr::load_espn_qbr(summary_type = "week"), "QBR_WEEK")
run_etl(con, nflreadr::load_ff_playerids(), "FF_PLAYERIDS")
run_etl(con, nflreadr::load_ftn_charting(), "FTN_CHARTING")
run_etl(con, nflreadr::load_nextgen_stats(stat_type = 'passing'), "NGS_PASSING")
run_etl(
  con,
  nflreadr::load_nextgen_stats(stat_type = 'receiving'),
  "NGS_RECEIVING"
)
run_etl(con, nflreadr::load_nextgen_stats(stat_type = 'rushing'), "NGS_RUSHING")
run_etl(
  con,
  nflreadr::load_ff_opportunity(stat_type = 'weekly'),
  'FF_OPPURTUNITY_WK'
)
run_etl(
  con,
  nflreadr::load_ff_opportunity(stat_type = 'pbp_pass'),
  'FF_OPPURTUNITY_PASS'
)
run_etl(
  con,
  nflreadr::load_ff_opportunity(stat_type = 'pbp_rush'),
  'FF_OPPURTUNITY_RUSH'
)
run_etl(con, nflreadr::load_officials(), "OFFICIALS")
run_etl(con, nflreadr::load_injuries(), 'INJURIES')
run_etl(con, nflreadr::load_schedules(), "SCHEDULES")
run_etl(con, nflreadr::load_trades(), "TRADES")
run_etl(con, nflreadr::load_snap_counts(), "SNAP_COUNTS")
run_etl(con, nflreadr::load_players(), "PLAYERS")
run_etl(
  con,
  nflreadr::load_pfr_advstats(stat_type = 'pass', summary_level = 'season'),
  'PFR_ADV_PASS'
)
run_etl(
  con,
  nflreadr::load_pfr_advstats(stat_type = 'pass', summary_level = 'season'),
  'PFR_ADV_PASS_SEASON'
)
run_etl(
  con,
  nflreadr::load_pfr_advstats(stat_type = 'rush', summary_level = 'season'),
  'PFR_ADV_RUSH_SEASON'
)
run_etl(
  con,
  nflreadr::load_pfr_advstats(stat_type = 'rec', summary_level = 'season'),
  'PFR_ADV_REC_SEASON'
)
DBI::dbDisconnect(con)

run_etl(
  con,
  nflfastR::calculate_stats(summary_level = "week", stat_type = "team"),
  "TEAM_STATS_WK"
)
run_etl(
  con,
  nflfastR::calculate_stats(summary_level = "season", stat_type = "team"),
  "TEAM_STATS_SEASON"
)
run_etl(
  con,
  nflfastR::calculate_stats(summary_level = "week", stat_type = "player"),
  "PLAYER_STATS_WK"
)
run_etl(
  con,
  nflfastR::calculate_stats(summary_level = "season", stat_type = "player"),
  "PLAYER_STATS_SEASON"
)

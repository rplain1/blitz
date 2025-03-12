df_teams <- nflreadr::load_teams() |>
  tibble::as_tibble()

con <- DBI::dbConnect(duckdb::duckdb(), Sys.getenv("DB_PATH"))

DBI::dbWriteTable(
  con,
  DBI::Id(schema = "BASE", table = 'TEAMS'),
  df_teams
)

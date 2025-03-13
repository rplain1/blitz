### Game lines
create_game_lines_table <- function(con) {
  dplyr::tbl(con, DBI::Id("BASE", "NFLFASTR_PBP")) |>
    dplyr::group_by(season, week, game_id, season_type, home_team, away_team) |>
    dplyr::summarise(
      spread_line = max(spread_line),
      total_line = max(total_line),
      home_score = max(home_score),
      away_score = max(away_score),
      result = max(result),
      .groups = 'drop'
    ) |>
    dplyr::collect() -> df_base

  df_stage <- df_base |>
    tidyr::pivot_longer(
      cols = c(home_team, away_team),
      names_to = 'home_away',
      values_to = 'team'
    ) |>
    dplyr::mutate(
      outcome = dplyr::case_when(
        result == 0 ~ 'TIE',
        home_away == 'home_team' & result > 0 ~ 'WIN',
        home_away == 'away_team' & result < 0 ~ 'WIN',
        home_away == 'home_team' & result < 0 ~ 'LOSS',
        home_away == 'away_team' & result > 0 ~ 'LOSS',
        TRUE ~ NA_character_
      ),
      score = dplyr::if_else(home_away == 'home_team', home_score, away_score),
      implied_score = dplyr::case_when(
        home_away == 'home_team' & spread_line > 0 ~
          (total_line + spread_line) / 2,
        home_away == 'away_team' & spread_line < 0 ~
          (total_line + spread_line) / 2,
        home_away == 'home_team' & spread_line < 0 ~
          (total_line - spread_line) / 2,
        home_away == 'away_team' & spread_line > 0 ~
          (total_line - spread_line) / 2,
        spread_line == 0 ~ total_line / 2,
        TRUE ~ NA_real_
      )
    ) |>
    dplyr::mutate(total_score = home_score + away_score) |>
    dplyr::select(
      season,
      week,
      game_id,
      season_type,
      team,
      home_away,
      spread_line,
      total_line,
      outcome,
      score,
      implied_score,
      total_score
    )

  df <- df_stage |>
    tidyr::pivot_wider(
      id_cols = c(season, week, game_id, season_type),
      names_from = home_away,
      values_from = team
    ) |>
    dplyr::left_join(df_stage)

  return(df)
}

### Plays ----------
create_plays_table <- function(con) {
  df_plays <- dplyr::tbl(con, DBI::Id("BASE", "NFLFASTR_PBP")) |>
    dplyr::filter(play == 1, penalty == 0) |>
    dplyr::mutate(
      head_coach = dplyr::if_else(posteam == home_team, home_coach, away_coach)
    ) |>
    dplyr::group_by(
      season,
      week,
      game_id,
      posteam,
      head_coach,
      spread_line,
      total_line
    ) |>
    dplyr::summarise(
      n = n(),
      passes = sum(pass),
      opp_pass_epa = mean(ifelse(pass == 1, epa, NA), na.rm = TRUE),
      .groups = 'drop'
    ) |>
    dplyr::mutate(pass_rate = passes / n)

  df_passers <- dplyr::tbl(con, DBI::Id("BASE", "NFLFASTR_PBP")) |>
    dplyr::filter(!is.na(passer_id)) |>
    dplyr::filter(play == 1, penalty == 0) |>
    dplyr::count(season, week, game_id, posteam, passer_id, passer) |>
    dplyr::collect() |>
    dplyr::group_by(posteam, game_id) |>
    dplyr::arrange(desc(n), .by_group = TRUE) |>
    dplyr::mutate(rn = dplyr::row_number()) |>
    dplyr::filter(rn == 1) |>
    dplyr::ungroup() |>
    dplyr::select(-rn, -n)

  df_plays_model <- df_plays |>
    dplyr::collect() |>
    dplyr::left_join(df_passers) |>
    dplyr::group_by(posteam) |>
    dplyr::arrange(season, week, .by_group = TRUE) |>
    dplyr::mutate(
      new_coach = ifelse(head_coach == dplyr::lag(head_coach), 0, 1),
      new_qb = ifelse(passer_id == dplyr::lag(passer_id), 0, 1)
    ) |>
    dplyr::ungroup() |>
    dplyr::rename(plays = n)

  return(df_plays_model)
}


# main functions ------------
create_summary_tables <- function(db_path = Sys.getenv("DB_PATH")) {
  write_summary_tables <- function(df, table_name) {
    DBI::dbWriteTable(
      con,
      DBI::Id("SUMMARY", table_name),
      df,
      overwrite = TRUE
    )
  }
  con <- DBI::dbConnect(duckdb::duckdb(), db_path)
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  DBI::dbExecute(con, "CREATE SCHEMA IF NOT EXISTS SUMMARY")

  create_game_lines_table(con) |> write_summary_tables("GAME_LINES")
  create_plays_table(con) |> write_summary_tables("PLAY_COUNTS")
}

library(readr)
library(dplyr)

skill_pos = read_csv('data/cum_stats/skill_pos_stats.csv')

max_season_df = skill_pos %>%
  group_by(id, name) %>%
  summarise(max_season = max(season)) %>%
  ungroup()

season_max = skill_pos %>%
  mutate(ms.rec = statREC/team_statREC,
         ms.rec_yds = rec_yds/team_rec_yds,
         ms.rec_td = rec_tds/team_rec_tds,
         est.yprr = rec_yds/team_attempts) %>%
  group_by(id, name) %>%
  summarise_if(is.numeric, max) %>%
  select(id, name, rush_yds, statREC, rec_yds, rec_tds, puntReturns_yds, kickReturns_yds, 
         ms.rec, ms.rec_yds, ms.rec_td, est.yprr) %>%
  filter(statREC > 30) %>%
  na.omit()
colnames(season_max) = paste0(colnames(season_max), "_max")

career = skill_pos %>%
  left_join(., max_season_df, by = c("id", "name")) %>%
  select(-season) %>%
  group_by(id, name, max_season) %>%
  summarise_if(is.numeric, sum) %>%
  filter(max_season > 2005)  %>%
  mutate(ms.rec = statREC/team_statREC,
         ms.rec_yds = rec_yds/team_rec_yds,
         ms.rec_td = rec_tds/team_rec_tds,
         est.yprr = rec_yds/team_attempts) %>%
  ungroup() %>%
  select(-name, -max_season, -team_statCAR, -team_rush_yds, -team_rush_tds, -team_statREC, -team_rec_yds, -team_rec_tds, -team_attempts)
colnames(career) = paste0(colnames(career), "_career")

df = season_max %>%
  inner_join(., career, by = c("id_max"="id_career"))


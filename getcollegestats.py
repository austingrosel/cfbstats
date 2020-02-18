import re
import time
import requests
import pandas as pd
import numpy as np
from functools import reduce
from pandas.io.json import json_normalize
from functions import *


ovr_start = time.time()
data_start = time.time()
seasons = [2004, 2005, 2006, 2007, 2008, 2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019]
season_types = ['regular', 'postseason']
total_records = pd.DataFrame()

for this_season in seasons:
    total_season_records = pd.DataFrame()
    print(this_season)
    for this_season_type in season_types:
        response = requests.get("https://api.collegefootballdata.com/games", params = {"year":this_season, 'seasonType': this_season_type})
        games = pd.read_json(response.text)
        if this_season_type == 'postseason':
            games = games[games.week < 3]
        total_week_records = pd.DataFrame()
        for this_week in games.week.unique():
            response = requests.get("https://api.collegefootballdata.com/games/players",
                                    params = {"year": this_season, "week": this_week, 'seasonType': this_season_type})
            records = json_normalize(
                response.json(),
                ['teams', 'categories', 'types', 'athletes'],
                ['id', ['teams', 'school'], ['teams', 'categories', 'name'], ['teams', 'categories', 'types', 'name']],
                meta_prefix='game.'
            )
            records['season'] = this_season
            records = records[(records.name != ' Team') & 
                              (records['name'] != 'Team') &
                              (records['name'] != '- Team') &
                              (records['stat'] != '--')].reset_index(drop=True)
            total_week_records = total_week_records.append(records)
        total_season_records = total_season_records.append(total_week_records)
    total_records = total_records.append(total_season_records)

total_records = total_records.reset_index(drop=True)
s = (time.time() - data_start)/60.0
print('*** Getting the data from the API took %.1f minutes. ***' % s)

team_stats_rec = build_skill_stat_dataframe(total_records, 'rec', team_level=True)
team_stats_rush = build_skill_stat_dataframe(total_records, 'rush', team_level=True)
team_stats_pass = build_skill_stat_dataframe(total_records, 'pass', team_level=True)
team_stats_def = build_defensive_dataframe(total_records, True)

dfs = [total_records, team_stats_pass, team_stats_rush, team_stats_rec, team_stats_def]
total_records = reduce(lambda left, right: pd.merge(left, right, on=['season', 'game.id', 'game.teams.school'], how='outer'), dfs)
total_records['id'] = total_records.id.astype(int)

passing_df = build_skill_stat_dataframe(total_records, skill_type='pass', team_level=False)

print('passing_df:',len(passing_df))

rushing_df = build_skill_stat_dataframe(total_records, skill_type='rush', team_level=False)[['id', 'season', 'rush_games', 'statCAR', 'rush_yds', 'rush_tds', 'team_statCAR', 'team_rush_yds', 'team_rush_tds']]

print('rushing_df:',len(rushing_df))

receiving_df = build_skill_stat_dataframe(total_records, skill_type='rec', team_level=False)[['id', 'season', 'rec_games', 'statREC', 'rec_yds', 'rec_tds', 'team_attempts', 'team_statREC', 'team_rec_yds', 'team_rec_tds']]
punt_return_df = build_skill_stat_dataframe(total_records, skill_type='puntReturns', team_level=False)[['id', 'season', 'statNO', 'puntReturns_yds', 'puntReturns_tds']]
kick_return_df = build_skill_stat_dataframe(total_records, skill_type='kickReturns', team_level=False)[['id', 'season', 'statNO', 'kickReturns_yds', 'kickReturns_tds']]
def_df = build_defensive_dataframe(total_records, team_level=False)[['id', 'season', 'def_games', 'int_games',
                                                                     'defPD', 'defQB HUR', 'defSACKS',
                                                                     'defSOLO', 'defTD', 'defTFL', 'defTOT', 'defINT',
                                                                     'team_defPD', 'team_defQB HUR', 'team_defSACKS', 'team_defSOLO',
                                                                     'team_defTD', 'team_defTFL', 'team_defTOT', 'team_defINT']]
fumble_df = build_fumble_dataframe(total_records)

dfs = [passing_df, rushing_df, fumble_df, receiving_df, punt_return_df, kick_return_df, def_df]
player_df = reduce(lambda left, right: pd.merge(left, right, on=['season', 'id'], how='outer'), dfs).fillna(0).astype(int)
player_df = player_df.assign(games=player_df.apply(how_many_games, axis=1)).rename(columns={'statNO_x': 'puntReturns', 'statNO_y': 'kickReturns'}).drop(['pass_games', 'rec_games', 'rush_games', 'int_games', 'def_games'], axis=1)
player_df = total_records[['id', 'season', 'name']].drop_duplicates().merge(player_df, on=['id', 'season'], how='inner')
del player_df['team_defINT']

# Get skill players stats
skill_pos_df = player_df[['id', 'season', 'name', 'games',
                          'statCAR', 'rush_yds', 'rush_tds', 'statFUM', 'team_statCAR', 'team_rush_yds', 'team_rush_tds',
                          'statREC', 'rec_yds', 'rec_tds', 'team_statREC', 'team_rec_yds', 'team_rec_tds', 'team_attempts',
                          'puntReturns', 'puntReturns_yds', 'puntReturns_tds',
                          'kickReturns', 'kickReturns_yds', 'kickReturns_tds']]
#game_ids = skill_pos_df.groupby(['id']).season.max().reset_index()

skill_pos_df = skill_pos_df.groupby(['id']).season.max().reset_index().merge(skill_pos_df.groupby(['id', 'name'])[skill_pos_df.columns[3:]].sum().reset_index(), on='id', how='outer')
print('skill_pos_df:',len(skill_pos_df))
skill_pos_df['ms.rush'] = (skill_pos_df.statCAR/skill_pos_df.team_statCAR).round(3)
skill_pos_df['ms.rush_yds'] = (skill_pos_df.rush_yds/skill_pos_df.team_rush_yds).round(3)
skill_pos_df['ypc'] = (skill_pos_df.rush_yds/skill_pos_df.statCAR).round(2)
skill_pos_df['ms.rec'] = (skill_pos_df.statREC/skill_pos_df.team_statREC).round(3)
skill_pos_df['ms.rec_yds'] = (skill_pos_df.rec_yds/skill_pos_df.team_rec_yds).round(3)
skill_pos_df['est_yprr'] = (skill_pos_df.rec_yds/skill_pos_df.team_attempts).round(3)
skill_pos_df['ypr'] = (skill_pos_df.rec_yds/skill_pos_df.statREC).round(1)
skill_pos_df.to_csv('skill_pos_stats.csv', index=False)

# Get QB stats
qb_df = player_df[['id', 'season', 'name', 'games',
                   'completions', 'attempts', 'pass_yds', 'pass_tds', 'statINT',
                   'statCAR', 'rush_yds', 'rush_tds', 'statFUM', 'team_statCAR', 'team_rush_yds', 'team_rush_tds']]
qb_df = qb_df.groupby(['id']).season.max().reset_index().merge(qb_df.groupby(['id', 'name'])[qb_df.columns[3:]].sum().reset_index(), on='id', how='outer')
qb_df['cmp.pct'] = (qb_df.completions/qb_df.attempts).round(3)
qb_df['ypa'] = (qb_df.pass_yds/qb_df.attempts).round(2)
qb_df['ypc'] = (qb_df.rush_yds/qb_df.statCAR).round(2)
qb_df.to_csv('qb_stats.csv', index=False)

# Get defensive player stats
def_player_df = player_df[['id', 'season', 'name', 'games', 'defPD', 'defQB HUR', 'defSACKS', 'defSOLO', 'defTD', 'defTFL', 'defTOT', 'defINT',
                           'team_defPD', 'team_defQB HUR', 'team_defSACKS', 'team_defSOLO', 'team_defTD', 'team_defTFL', 'team_defTOT'
                          ]]
def_player_df = def_player_df.groupby(['id']).season.max().reset_index().merge(def_player_df.groupby(['id', 'name'])[def_player_df.columns[3:]].sum().reset_index(), on='id', how='outer')
print('def_player:',len(def_player_df))
def_player_df['ms.tck'] = (def_player_df.defTOT/def_player_df.team_defTOT).round(3)
def_player_df['tk.game'] = (def_player_df.defTOT/def_player_df.games).round(1)
def_player_df['ms.sck'] = (def_player_df.defSACKS/def_player_df.team_defSACKS).round(3)
def_player_df['sk.game'] = (def_player_df.defSACKS/def_player_df.games).round(1)
def_player_df['ms.tfl'] = (def_player_df.defTFL/def_player_df.team_defTFL).round(3)
def_player_df['tfl.game'] = (def_player_df.defTFL/def_player_df.games).round(1)
def_player_df['ms.qbhur'] = (def_player_df['defQB HUR']/def_player_df['team_defQB HUR']).round(3)
def_player_df = def_player_df.replace([np.inf, -np.inf], np.nan)
def_player_df.to_csv('def_stats.csv', index=False)

s = (time.time() - ovr_start)/60.0
print('*** The entire script took %.1f minutes. ***' % s)

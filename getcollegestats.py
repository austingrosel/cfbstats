import re
import time
import requests
import argparse
import os
import glob
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize
from functools import reduce
from player_functions import *
from scrape_functions import *

#####################
### BUILD DATASET ###
#####################


def main():
    ovr_start = time.time()
    data_start = time.time()

    # Check if there is a year argument passed.
    parser = argparse.ArgumentParser()
    parser.add_argument('--year', type=int)
    args = parser.parse_args()

    # If there was a year argument passed, only scrape that season. Else, get the history.
    if args.year is not None:
        seasons = [args.year]
    else:
        seasons = list(range(2004, 2021))

    # Build the necessary data folders for outputting the CSVs
    if not os.path.exists('data/'):
        os.makedirs('data/')
    if not os.path.exists('data/season_stats/'):
        os.makedirs('data/season_stats/')
    if not os.path.exists('data/cum_stats/'):
        os.makedirs('data/cum_stats/')
    if not os.path.exists('data/ratings/'):
        os.makedirs('data/ratings/')
    if not os.path.exists('data/roster/'):
        os.makedirs('data/roster/')

    # Output the game stats into CSVs
    for this_season in seasons:
        scrape_game_stats(this_season)
        scrape_ratings(this_season)

    total_records = pd.concat(map(pd.read_csv, glob.glob(os.path.join('', "data/season_stats/*.csv")))).reset_index(drop=True)
    total_records['id'] = total_records.id.astype(int)

    s = (time.time() - data_start)/60.0
    print('*** Getting the data from the API took %.1f minutes. ***' % s)

    ########################
    ### BUILD TEAM STATS ###
    ########################

    team_stats_rec = build_skill_stat_dataframe(total_records, 'rec', team_level=True)
    team_stats_rush = build_skill_stat_dataframe(total_records, 'rush', team_level=True)
    team_stats_pass = build_skill_stat_dataframe(total_records, 'pass', team_level=True)
    team_stats_def = build_defensive_dataframe(total_records, True)

    dfs = [total_records, team_stats_pass, team_stats_rush, team_stats_rec, team_stats_def]
    team_stats = reduce(lambda left, right: pd.merge(left, right, on=['season', 'game.id', 'game.teams.school'], how='outer'), dfs).fillna(0)

    ##############################
    ### BUILD INDIVIDUAL STATS ###
    ##############################

    passing_df = build_skill_stat_dataframe(team_stats, skill_type='pass', team_level=False)
    rushing_df = build_skill_stat_dataframe(team_stats, skill_type='rush', team_level=False)[['id', 'season', 'rush_games', 'statCAR', 'rush_yds', 'rush_tds', 'team_statCAR', 'team_rush_yds', 'team_rush_tds']]
    receiving_df = build_skill_stat_dataframe(team_stats, skill_type='rec', team_level=False)[['id', 'season', 'rec_games', 'statREC', 'rec_yds', 'rec_tds', 'team_attempts', 'team_statREC', 'team_rec_yds', 'team_rec_tds']]
    punt_return_df = build_skill_stat_dataframe(team_stats, skill_type='puntReturns', team_level=False)[['id', 'season', 'statNO', 'puntReturns_yds', 'puntReturns_tds']]
    kick_return_df = build_skill_stat_dataframe(team_stats, skill_type='kickReturns', team_level=False)[['id', 'season', 'statNO', 'kickReturns_yds', 'kickReturns_tds']]
    def_df = build_defensive_dataframe(team_stats, team_level=False)[['id', 'season', 'def_games', 'int_games',
                                                                      'defPD', 'defQB HUR', 'defSACKS',
                                                                      'defSOLO', 'defTD', 'defTFL', 'defTOT', 'defINT',
                                                                      'team_defPD', 'team_defQB HUR', 'team_defSACKS', 'team_defSOLO',
                                                                      'team_defTD', 'team_defTFL', 'team_defTOT', 'team_defINT']]
    fumble_df = build_fumble_dataframe(team_stats)

    dfs = [passing_df, rushing_df, fumble_df, receiving_df, punt_return_df, kick_return_df, def_df]
    player_df = reduce(lambda left, right: pd.merge(left, right, on=['season', 'id'], how='outer'), dfs).fillna(0).astype(int)
    player_df = player_df.assign(games=player_df.apply(how_many_games, axis=1)).rename(columns={'statNO_x': 'puntReturns', 'statNO_y': 'kickReturns'}).drop(['pass_games', 'rec_games', 'rush_games', 'int_games', 'def_games'], axis=1)
    player_df = total_records[['id', 'season', 'name']].drop_duplicates().merge(player_df, on=['id', 'season'], how='inner')
    del player_df['team_defINT']
    player_df.to_csv('data/cum_stats/ind_stats.csv', index=False)

    ##############################
    ### GET SKILL PLAYER STATS ###
    ##############################

    skill_pos_df = get_position_stats(player_df, columns=['id', 'season', 'name', 'games',
                              'statCAR', 'rush_yds', 'rush_tds', 'statFUM', 'team_statCAR', 'team_rush_yds', 'team_rush_tds',
                              'statREC', 'rec_yds', 'rec_tds', 'team_statREC', 'team_rec_yds', 'team_rec_tds', 'team_attempts',
                              'puntReturns', 'puntReturns_yds', 'puntReturns_tds',
                              'kickReturns', 'kickReturns_yds', 'kickReturns_tds'])
    skill_pos_df.to_csv('data/cum_stats/skill_pos_stats.csv', index=False)

    ####################
    ### GET QB STATS ###
    ####################

    qb_df = get_position_stats(player_df, columns=['id', 'season', 'name', 'games', 'completions', 'attempts', 'pass_yds', 'pass_tds', 'statINT',
                       'statCAR', 'rush_yds', 'rush_tds', 'statFUM', 'team_statCAR', 'team_rush_yds', 'team_rush_tds'])
    qb_df.to_csv('data/cum_stats/qb_stats.csv', index=False)

    ############################
    ### GET DEF PLAYER STATS ###
    ############################

    def_player_df = get_position_stats(player_df, columns=['id', 'season', 'name', 'games', 
                               'defPD', 'defQB HUR', 'defSACKS', 'defSOLO', 'defTD', 'defTFL', 'defTOT', 'defINT',
                               'team_defPD', 'team_defQB HUR', 'team_defSACKS', 'team_defSOLO', 'team_defTD', 'team_defTFL', 'team_defTOT'])
    def_player_df.to_csv('data/cum_stats/def_stats.csv', index=False)

    s = (time.time() - ovr_start)/60.0
    print('*** The entire script took %.1f minutes. ***' % s)


if __name__ == "__main__":
    main()

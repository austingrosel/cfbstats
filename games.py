import random
import luigi
import time
import requests
import itertools
from collections import defaultdict
from luigi import six
import pandas as pd
from datetime import datetime
from functools import reduce
from pandas.io.json import json_normalize

from functions import *

class Streams(luigi.Task):
    season = luigi.YearParameter()
    season_type = luigi.Parameter()

    def run(self):
        s = "https://api.collegefootballdata.com/games?year={}&seasonType={}".format(self.season.year, self.season_type)
        print('****** here is the link:', s)

        response = requests.get(s)
        tbl = pd.read_json(response.text)

        with self.output().open('w') as fout:
            tbl.to_csv(fout, index=False)

    def output(self):
        return luigi.LocalTarget('games/games_{}_{}.csv'.format(self.season.year, self.season_type))


class GetGameIDs(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        season_types = ['regular', 'postseason']
        return [Streams(date, season_type) for date, season_type in itertools.product(self.date_interval, season_types)]

    def run(self):
        game_ids = []
        for t in self.input():
            with t.open('r') as in_file:
                for line in in_file:
                    line = line.strip().split(',')
                    season, week, season_type = line[1], line[2], line[3]
                    if season != 'season':
                        game_ids.append([season, week, season_type])
        game_ids = [list(x) for x in set(tuple(x) for x in game_ids)]

        with self.output().open('w') as out_file:
            for i in game_ids:
                out_file.write('{},{},{}\n'.format(i[0], i[1], i[2]))

    def output(self):
        return luigi.LocalTarget("games/game_ids.csv")


class GetAllStats(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return GetGameIDs(self.date_interval)

    def run(self):
        with self.input().open('r') as infile:
            lines = infile.read().splitlines()

        with self.output().open('w') as outfile:
            stats_list = []
            for l in lines:
                line = l.strip().split(',')
                print(line)

                response = requests.get("https://api.collegefootballdata.com/games/players",
                                        params = {"year": line[0], "week": line[1], "seasonType": line[2]})

                records = json_normalize(
                    response.json(),
                    ['teams', 'categories', 'types', 'athletes'],
                    ['id', ['teams', 'school'], ['teams', 'categories', 'name'], ['teams', 'categories', 'types', 'name']],
                    meta_prefix='game.'
                )
                records['season'] = line[0]
                if len(records) > 0:
                    records = records[(records.name != ' Team') &
                                      (records['name'] != 'Team') &
                                      (records['name'] != '- Team') &
                                      (records['stat'] != '--')].reset_index(drop=True)
                    stats_list.append(records)
            stats = pd.concat(stats_list, ignore_index=True)
            stats.to_csv(outfile, index=False)

    def output(self):
        return luigi.LocalTarget("stats/all_stats.csv")


class GetTeamStats(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return GetAllStats(self.date_interval)

    def run(self):
        df = pd.read_csv(self.input().open('r'))

        team_stats_rec = build_skill_stat_dataframe(df, 'rec', team_level=True)
        team_stats_rush = build_skill_stat_dataframe(df, 'rush', team_level=True)
        team_stats_pass = build_skill_stat_dataframe(df, 'pass', team_level=True)
        team_stats_def = build_defensive_dataframe(df, True)

        dfs = [df, team_stats_pass, team_stats_rush, team_stats_rec, team_stats_def]
        df = reduce(lambda left, right: pd.merge(left, right, on=['season', 'game.id', 'game.teams.school'], how='outer'), dfs).fillna(0)
        
        with self.output().open('w') as outfile:
            df.to_csv(outfile, index=False)

    def output(self):
        return luigi.LocalTarget("stats/team_stats.csv")


class GetIndividualStats(luigi.Task):
    date_interval = luigi.DateIntervalParameter()

    def requires(self):
        return GetTeamStats(self.date_interval)

    def run(self):
        df = pd.read_csv(self.input().open('r'))

        passing_df = build_skill_stat_dataframe(df, skill_type='pass', team_level=False)
        rushing_df = build_skill_stat_dataframe(df, skill_type='rush', team_level=False)[['id', 'season', 'rush_games', 'statCAR', 'rush_yds', 'rush_tds', 'team_statCAR', 'team_rush_yds', 'team_rush_tds']]
        receiving_df = build_skill_stat_dataframe(df, skill_type='rec', team_level=False)[['id', 'season', 'rec_games', 'statREC', 'rec_yds', 'rec_tds', 'team_attempts', 'team_statREC', 'team_rec_yds', 'team_rec_tds']]
        punt_return_df = build_skill_stat_dataframe(df, skill_type='puntReturns', team_level=False)[['id', 'season', 'statNO', 'puntReturns_yds', 'puntReturns_tds']]
        kick_return_df = build_skill_stat_dataframe(df, skill_type='kickReturns', team_level=False)[['id', 'season', 'statNO', 'kickReturns_yds', 'kickReturns_tds']]
        def_df = build_defensive_dataframe(df, team_level=False)[['id', 'season', 'def_games', 'int_games',
                                                                     'defPD', 'defQB HUR', 'defSACKS',
                                                                     'defSOLO', 'defTD', 'defTFL', 'defTOT', 'defINT',
                                                                     'team_defPD', 'team_defQB HUR', 'team_defSACKS', 'team_defSOLO',
                                                                     'team_defTD', 'team_defTFL', 'team_defTOT', 'team_defINT']]
        fumble_df = build_fumble_dataframe(df)

        dfs = [passing_df, rushing_df, fumble_df, receiving_df, punt_return_df, kick_return_df, def_df]
        player_df = reduce(lambda left, right: pd.merge(left, right, on=['season', 'id'], how='outer'), dfs).fillna(0).astype(int)
        player_df = player_df.assign(games=player_df.apply(how_many_games, axis=1)).rename(columns={'statNO_x': 'puntReturns', 'statNO_y': 'kickReturns'}).drop(['pass_games', 'rec_games', 'rush_games', 'int_games', 'def_games'], axis=1)
        player_df = df[['id', 'season', 'name']].drop_duplicates().merge(player_df, on=['id', 'season'], how='inner')
        del player_df['team_defINT']
        
        with self.output().open('w') as outfile:
            player_df.to_csv(outfile, index=False)

    def output(self):
        return luigi.LocalTarget("stats/ind_stats.csv")


if __name__ == "__main__":
    luigi.run()
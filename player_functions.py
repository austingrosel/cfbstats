import re
import pandas as pd
import numpy as np


def get_position_stats(df, columns):
    df = df[columns]
    df = df.groupby(['season', 'id', 'name'])[df.columns[3:]].sum().reset_index()
    df = df.replace([np.inf, -np.inf], np.nan)
    print('len df: {}'.format(len(df)))
    return df


def standardize_df(df, this_type, team_level):
    if this_type == 'fumbles':
        df = df[df['game.teams.categories.name'] == 'fumbles'].reset_index(drop=True)
        df['stat'] = df.stat.astype(int)
    elif this_type == 'interceptions':
        df = df[df['game.teams.categories.name'] == 'interceptions'].reset_index(drop=True)
        df['stat'] = df.stat.astype(int)
    elif this_type == 'defensive':
        df = df[df['game.teams.categories.name'] == 'defensive'].reset_index(drop=True)
        df['stat'] = df.stat.astype(float)
    
    if team_level is False:
        cols = df.columns[df.columns.str.contains('team_')].tolist()
        team_df = df.drop_duplicates(['id', 'game.id', 'game.teams.school', 'team_completions'])
        team_df = team_df.groupby(['season', 'id'])[cols].sum().reset_index()
        
        df = df.groupby(['season', 'id', 'game.teams.categories.types.name']).agg(
            stat=('stat', sum),
            games=('game.id','count')
        ).reset_index()
        
        df = df.set_index(['season', 'id', 'games', 'game.teams.categories.types.name']).unstack().reset_index(level=[0,1])
    else:
        df = df.groupby(['season', 'game.id', 'game.teams.school', 'game.teams.categories.types.name']).agg(
            stat=('stat', sum)
        ).reset_index()
        df = df.set_index(['season', 'game.id', 'game.teams.school', 'game.teams.categories.types.name']).unstack().reset_index(level=[0,1])
    
    df = df.reset_index()
    df.columns = [''.join(col).strip() for col in df.columns.values]
    
    if team_level is False:
        df = df.merge(team_df, on=['season', 'id'], how='left')
    
    return df


def build_int_dataframe(df, team_level):
    df = standardize_df(df, 'interceptions', team_level)
    
    if team_level is False:
        df = df.rename(columns={'games':'int_games'})
        df = df[['season', 'id', 'int_games', 'statINT']]
    else:
        df = df[['season', 'game.id', 'game.teams.school', 'statINT']]
    return df


def build_defensive_dataframe(df, team_level):
    def_df = standardize_df(df, 'defensive', team_level)
    int_df = build_int_dataframe(df, team_level)
    
    if team_level is False:
        df = def_df.merge(int_df, on=['season', 'id'], how='outer').fillna(0)
        df = df.rename(columns=lambda x: re.sub('stat','def',x))
        df = df.rename(columns={'games':'def_games'})
    else:
        df = def_df.merge(int_df, on=['season', 'game.id', 'game.teams.school'], how='outer').fillna(0)
        df = df.rename(columns=lambda x: re.sub('stat','team_def', x))

    return df


def build_fumble_dataframe(df):
    df = standardize_df(df, 'fumbles', False)
    return df[['season', 'id', 'statFUM']]


def build_skill_stat_dataframe(df, skill_type, team_level):
    if skill_type == 'rec':
        category = 'REC'
        stat_filter = 'receiving'
    elif skill_type == 'rush':
        category = 'CAR'
        stat_filter = 'rushing'
    elif skill_type == 'puntReturns':
        category = 'NO'
        stat_filter = 'puntReturns'
    elif skill_type == 'kickReturns':
        category = 'NO'
        stat_filter = 'kickReturns'
    else:
        category = 'INT'
        stat_filter = 'passing'
        pass_cpa_df = df[(df['game.teams.categories.name'] == 'passing') &
                         (df['game.teams.categories.types.name'] == 'C/ATT')].reset_index(drop=True)
        pass_cpa_df[['C','ATT']] = pass_cpa_df['stat'].str.split('/',expand=True)
        pass_cpa_df['C'] = pass_cpa_df.C.astype(int)
        pass_cpa_df['ATT'] = pass_cpa_df.ATT.astype(int)
        
        if team_level is False:
            pass_cpa_df = pass_cpa_df.groupby(['season', 'id']).agg(
                completions=('C', sum),
                attempts=('ATT',sum)
            ).reset_index()
        else:
            pass_cpa_df = pass_cpa_df.groupby(['season', 'game.id', 'game.teams.school']).agg(
                team_completions=('C', sum),
                team_attempts=('ATT',sum)
            ).reset_index()
    
    df = df[(df['game.teams.categories.name'] == stat_filter) & 
            ((df['game.teams.categories.types.name'] == 'YDS') |
            (df['game.teams.categories.types.name'] == category) |
            (df['game.teams.categories.types.name'] == 'TD'))].reset_index(drop=True)
    
    df['stat'] = df.stat.astype(int)
    
    del df['name']
    
    if team_level is False:
        cols = df.columns[df.columns.str.contains('team_')].tolist()
        team_df = df.drop_duplicates(['id', 'game.id', 'game.teams.school'])
        team_df = team_df.groupby(['season', 'id'])[cols].sum().reset_index()
        
        game_stats_df = df.groupby(['season', 'id', 'game.teams.categories.types.name']).agg(
            stat=('stat', sum),
            games=('game.id','count')
        ).reset_index()
        
        df = game_stats_df.set_index(['season', 'games', 'id', 'game.teams.categories.types.name']).unstack().reset_index(level=[0,1])
    else:
        df = df.groupby(['season', 'game.id', 'game.teams.school', 'game.teams.categories.types.name']).agg(
            stat=('stat', sum)
        ).reset_index()

        df = df.set_index(['season', 'game.id', 'game.teams.school', 'game.teams.categories.types.name']).unstack().reset_index(level=[0,1])
    
    df = df.reset_index()
    df.columns = [''.join(col).strip() for col in df.columns.values]
    
    col_name = 'stat' + category
    
    if team_level is False:
        df = df.merge(team_df, on=['season', 'id'], how='left')
        if skill_type == 'pass':
            df = df.merge(pass_cpa_df, how='left', on=['season', 'id'])
            df = df[['season', 'id', 'games', 'completions', 'attempts', 'statYDS', 'statTD', col_name, 'team_completions']]
        #else:
        #df = df[['season', 'id', 'games', col_name, 'statYDS', 'statTD']]
        df = df.rename(columns={"games": skill_type + '_games',
                                "statYDS": skill_type + '_yds',
                                "statTD": skill_type + '_tds'})
    else:
        if skill_type == 'pass':
            df = df.merge(pass_cpa_df, how='left', on=['season', 'game.id', 'game.teams.school'])
            df = df[['season', 'game.id', 'game.teams.school', 'team_completions', 'team_attempts', 'statYDS', 'statTD', col_name]]
        else:
            df = df[['season', 'game.id', 'game.teams.school', col_name, 'statYDS', 'statTD']]
        df = df.rename(columns={col_name: 'team_' + col_name,
                                "statYDS": 'team_' + skill_type + '_yds',
                                "statTD": 'team_' + skill_type + '_tds'})

    return df


def how_many_games(x):
    if (x.pass_games >= x.rush_games) and (x.pass_games >= x.rec_games) and (x.pass_games >= x.def_games) and (x.pass_games >= x.int_games):
        return x.pass_games
    elif (x.rush_games >= x.pass_games) and (x.rush_games >= x.rec_games) and (x.rush_games >= x.def_games) and (x.rush_games >= x.int_games):
        return x.rush_games
    elif (x.rec_games >= x.pass_games) and (x.rec_games >= x.rush_games) and (x.rec_games >= x.def_games) and (x.rec_games >= x.int_games):
        return x.rec_games
    elif (x.int_games >= x.pass_games) and (x.int_games >= x.rush_games) and (x.int_games >= x.rec_games) and (x.int_games >= x.def_games):
        return x.int_games
    elif (x.def_games >= x.pass_games) and (x.def_games >= x.rush_games) and (x.def_games >= x.rec_games) and (x.def_games >= x.int_games):
        return x.def_games
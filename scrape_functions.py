import requests
import pandas as pd
from pandas.io.json import json_normalize


def scrape_game_stats(this_season):
    print(this_season)
    for this_season_type in ['regular', 'postseason']:
        response = requests.get("https://api.collegefootballdata.com/games", params = {"year": this_season, 'seasonType': this_season_type})
        games = pd.read_json(response.text)
        if len(games) == 0:
            print('*** Could not get games for season: {}, seasonType: {}.'.format(this_season, this_season_type))
            continue
        if this_season_type == 'postseason':
            games = games[games.week < 3]
        for this_week in games.week.unique():
            response = requests.get("https://api.collegefootballdata.com/games/players", params = {"year": this_season, "week": this_week, 'seasonType': this_season_type})
            try:
                records = json_normalize(
                  response.json(),
                  ['teams', 'categories', 'types', 'athletes'],
                  ['id', ['teams', 'school'], ['teams', 'categories', 'name'], ['teams', 'categories', 'types', 'name']],
                meta_prefix='game.')
                records['season'] = this_season
                records = records[(records.name != ' Team') & (records['name'] != 'Team') & (records['name'] != '- Team') & (records['stat'] != '--')].reset_index(drop=True)
                records.to_csv('data/season_stats/week_{}_stats_{}_{}.csv'.format(this_week, this_season, this_season_type))
            except:
                print('*** Something went wrong for season: {}, week: {}, seasonType: {}.'.format(this_season, this_week, this_season_type))


def scrape_ratings(this_season):
    response = requests.get("https://api.collegefootballdata.com/ratings/srs", params = {"year": this_season})
    ratings = pd.read_json(response.text)
    if len(ratings) > 0:
        ratings.to_csv('data/ratings/ratings_{}.csv'.format(this_season), index=False)
    else:
        print('*** Could not get ratings for season: {}.'.format(this_season))
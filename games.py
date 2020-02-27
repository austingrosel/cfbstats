import random
import luigi
import requests
from collections import defaultdict

from luigi import six

import pandas as pd
from datetime import datetime

class Streams(luigi.Task):
    """
    Faked version right now, just generates bogus data.
    """
    season = luigi.YearParameter()

    def run(self):
        """
        Generates bogus data and writes it into the :py:meth:`~.Streams.output` target.
        """
        s = "https://api.collegefootballdata.com/games?year={}&seasonType=regular".format(self.season.year)
        print('****** here is the link:', s)

        response = requests.get(s)
        tbl = pd.read_json(response.text)

        with self.output().open('w') as fout:
            tbl.to_csv(fout, index=False)

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file in the local file system.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget('games/games_{}.csv'.format(self.season.year))


class AggregateTeams(luigi.Task):
    """
    This task runs over the target data returned by :py:meth:`~/.Streams.output` and
    writes the result into its :py:meth:`~.AggregateArtists.output` target (local file).
    """

    #year = luigi.YearParameter(default=datetime.strptime('2014', '%Y'))
    date_interval = luigi.DateIntervalParameter()

    def output(self):
        """
        Returns the target output for this task.
        In this case, a successful execution of this task will create a file on the local filesystem.
        :return: the target output for this task.
        :rtype: object (:py:class:`luigi.target.Target`)
        """
        return luigi.LocalTarget("games/team_streams_{}.csv".format(self.date_interval))

    def requires(self):
        """
        This task's dependencies:
        * :py:class:`~.Streams`
        :return: list of object (:py:class:`luigi.task.Task`)
        """
        return [Streams(date) for date in self.date_interval]

    def run(self):
        home_team_count = defaultdict(int)

        for t in self.input():
            with t.open('r') as in_file:
                for line in in_file:
                    team = line.strip().split(',')[10]
                    home_team_count[team] += 1

        with self.output().open('w') as out_file:
            for team, count in six.iteritems(home_team_count):
                out_file.write('{},{}\n'.format(team, count))


if __name__ == "__main__":
    luigi.run()
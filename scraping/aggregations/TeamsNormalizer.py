from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger
import pandas as pd
import os


class TeamsNormalizer:

    def __init__(self):
        self.mongo_wrapper = PrefixedMongoWrapper('teams_inventory')
        self.master = self._get_laliga_teams()
        self.default_csv_filename = './teams_mapping.csv'
        self.logger = Logger(0)

        self.source_collections = {
            'laliga': 'laliga_web_primera_results',
            'as': 'as_classifications_data_classification',
            'marca': 'marca_current_season_results'
        }



    def find_team_id(self, source, team):
        self.data = self._get_raw_data()
        results = self.data.loc[self.data[source] == team]

        for result in results['master']:
            return result

        return ''

    def _get_raw_data(self):
        if not os.path.isfile(self.default_csv_filename):
            data = self.normalize()
            self.save_csv(data)


        return pd.read_csv(self.default_csv_filename)

    def _get_laliga_teams(self):
        reader =  PrefixedMongoWrapper('laliga_web')
        return reader.get_collection('primera_results').find().distinct("home")

    def _get_marca_teams(self):
        result = []
        reader =  PrefixedMongoWrapper('marca')
        days = reader.get_collection('current_season_results').find()
        for day in days:
            for day_result in day['results']:
                result.append(day_result['home'])


        return list(set(result))

    def _get_as_teams(self):
        reader =  PrefixedMongoWrapper('as_classifications_data')
        return reader.get_collection('classification').find().distinct("team")


    def normalize(self):
        self.logger.debug('Normalizing data...')
        result = {
            'master': self._get_laliga_teams(),
            'marca': self._normalize_one(self._get_marca_teams()),
            'as': self._normalize_one(self._get_as_teams())
        }

        return result

    def save_csv(self, result):
        self.logger.debug('Creating ' + self.default_csv_filename)
        csv_filename = self.default_csv_filename

        repo = pd.DataFrame(result)
        repo.to_csv(csv_filename)


    def _normalize_one(self, teams):


        from difflib import SequenceMatcher
        result = []


        for master_team in self._get_laliga_teams():

            best_similarity = 0
            matched = ''
            for team in teams:


                matcher = SequenceMatcher(None, master_team, team)
                similarity = matcher.ratio()
                if (similarity > best_similarity) and (similarity > 0.7):
                    best_similarity = similarity
                    matched = team

            result.append(matched)
        return result

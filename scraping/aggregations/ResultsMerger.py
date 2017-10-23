from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger

import scraping.laliga.utils
import pymongo


class ResultsMerger:

    def __init__(self):

        self.logger = Logger(0)
        self.mongo_wrapper = PrefixedMongoWrapper('aggregated_match_results')

        self.template = {'season': 'primera/2017-18',
                         'day_num': '',
                         'home': '',
                         'away': '',
                         'score_home': '',
                         'score_away': ''}


    def _get_archive_results(self):

        wrapper = scraping.laliga.utils.create_mongo_writer()
        #archive = wrapper.get_collection('ordered_matches').find({'day_yyyymmgg': {"$gt": '20170101'}}).sort([('day_yyyymmgg', pymongo.ASCENDING)])
        archive = wrapper.get_collection('ordered_matches').find().sort([('day_yyyymmgg', pymongo.ASCENDING)])
        result = []

        #el historico
        for archive_match in archive:

            entry = self.template.copy()
            entry['season'] = archive_match['season']
            entry['day_num'] = int(archive_match['day_num'])
            entry['home'] = archive_match['home']
            entry['away'] = archive_match['away']
            entry['score_home'] = int(archive_match['score_home'])
            entry['score_away'] = int(archive_match['score_away'])


            result.append(entry)

        return result

    def merge(self):
        results = []
        results += self._get_archive_results()
        results += self._get_current_results()

        return results

    def save(self, result):
        wrapper = PrefixedMongoWrapper('etl_results')
        wrapper.drop_collection('all')
        wrapper.write_dictionaries_list('all', result)


    def _extract_scores(self, text):

        result = {}
        splitted = text.split('-')

        if len(splitted) == 2:
            if (len(splitted[0]) <= 2) and (len(splitted[1]) <= 2):
                result['score_home'] = int(splitted[0])
                result['score_away'] = int(splitted[1])

        return result

    def _get_current_results(self):
        result = []
        wrapper = PrefixedMongoWrapper('marca')

        #los actuales bajados por la web de marca
        for day in wrapper.get_collection('current_season_results').find():
            for match in day['results']:

                entry = self.template.copy()
                entry['day_num'] = int(day['day']['num_day'].replace('Jornada', '').strip())
                entry['home'] = match['home']
                entry['away'] = match['away']

                scores = self._extract_scores(match['result'])

                if len(scores) == 2:
                    entry['score_home'] = scores['score_home']
                    entry['score_away'] = scores['score_away']
                else:
                    entry['score_home'] = match['result']
                    entry['score_away'] = match['result']


                result.append(entry)
        return result









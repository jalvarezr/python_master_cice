from scraping.core.prefixed_mongo_wrapper import PrefixedMongoWrapper
from scraping.core.stdout_logger import Logger

import pymongo

class MatchesFactsAggregator:
    '''
    Genera la tabla de facts partidos en csv.

    En self.counters se van actualizando los valores incrementales, tipo los goles marcados a la fecha.

    Para modificar los ficheros que genera hay que definir el template de los registros que se van
    generando en self.counter_template, y poner conforme los valores en _update_counters
    '''
    def __init__(self):
        self.prefix = 'etl_results'
        self.mongo_wrapper = PrefixedMongoWrapper(self.prefix)
        self.collection = 'all'
        self.results = []

        self.counters = {}
        self.logger = Logger(2)

        #Diccionario con los datos recientes. Ver self._update_counters
        self.teams_recent_history = {}
        self.counter_template = {
            'played_home': 0,
            'played_away': 0,

            'score_competition_home': 0,
            'score_competition_away': 0,

            'matches_won_home': 0,
            'matches_won_away': 0,

            'matches_tied_home': 0,
            'matches_tied_away': 0,

            'matches_lost_home': 0,
            'matches_lost_away': 0,

            'goals_scored_home': 0,
            'goals_scored_away': 0,

            'goals_conceded_home': 0,
            'goals_conceded_away': 0,

            'num_days_without_goals': 0,
            'num_days_without_victory': 0,
            'ranking': 0

        }


    def _update_counters(self, match):

        match_winner = self._winner(match)

        #Goles hechos por cada equipo
        self._add_to_counter(match['home'], 'goals_scored_home', match['score_home'])
        self._add_to_counter(match['away'], 'goals_scored_away', match['score_away'])

        self._add_to_counter(match['home'], 'goals_conceded_home', match['score_away'])
        self._add_to_counter(match['away'], 'goals_conceded_away', match['score_home'])

        #añado al historiar los goles hechos
        self.teams_recent_history[match['home']]['goals'].append(int(match['score_home']))
        self.teams_recent_history[match['away']]['goals'].append(int(match['score_away']))

        #Suma de los goles hechos en los últimos 5 días
        self._set_counter(match['home'], 'ranking', sum(self.teams_recent_history[match['home']]['goals'][-5:]))
        self._set_counter(match['away'], 'ranking', sum(self.teams_recent_history[match['away']]['goals'][-5:]))

        #Partidos jugados
        self._add_to_counter(match['home'], 'played_home', 1)
        self._add_to_counter(match['away'], 'played_away', 1)

        #Partidos ganados, empatados, perdidos
        key_map = {'home': 'matches_won_home', 'away': 'matches_lost_home', 'none': 'matches_tied_home'}
        self._add_to_counter(match['home'], key_map[match_winner], 1)

        key_map = {'home': 'matches_lost_away', 'away': 'matches_won_away', 'none': 'matches_tied_away'}
        self._add_to_counter(match['away'], key_map[match_winner], 1)

        #Puntos
        key_map = {'home': 3, 'away': 0, 'none': 1}
        self._add_to_counter(match['home'], 'score_competition_home', key_map[match_winner])

        key_map = {'home': 0, 'away': 3, 'none': 1}
        self._add_to_counter(match['away'], 'score_competition_away', key_map[match_winner])

        #Días sin ganar
        if match_winner == 'home':
            self._set_counter(match['home'], 'num_days_without_victory', 0)
            self._add_to_counter(match['away'], 'num_days_without_victory', 1)

        if match_winner == 'away':
            self._set_counter(match['away'], 'num_days_without_victory', 0)
            self._add_to_counter(match['home'], 'num_days_without_victory', 1)

        if match_winner == 'none':
            self._add_to_counter(match['home'], 'num_days_without_victory', 1)
            self._add_to_counter(match['away'], 'num_days_without_victory', 1)

        #Días sin marcar
        if int(match['score_home']) > 0:
            self._set_counter(match['home'], 'num_days_without_goals', 0)
        else:
            self._add_to_counter(match['home'], 'num_days_without_goals', 1)

        if int(match['score_away']) > 0:
            self._set_counter(match['away'], 'num_days_without_goals', 0)
        else:
            self._add_to_counter(match['away'], 'num_days_without_goals', 1)



    def process_matches_played(self, season):
        self._init_counters()
        for match in self._collection().find({'season': season}).sort([('day_num', pymongo.ASCENDING)]):
            entry = self._process_match(match)

            entry['season'] = match['season']

            entry['team_home'] = match['home']
            entry['team_away'] = match['away']

            entry['winner'] = self._winner(match)

            self._add_to_results(entry)



    def write_data_mongo(self):
        '''
        Escribe en mongo los resultados del proceso
        :return:
        '''
        self.mongo_wrapper.drop_collection('aggregated_results')
        self.mongo_wrapper.write_dictionaries_list('aggregated_results', self.results)

    def write_data_csv(self, filename):
        '''
        Exporta a csv los resultados del proceso
        :param filename:
        :return:
        '''
        import pandas as pd

        data = {}
        for column in self.results[0].keys():
            data[column] = []

        for result in self.results:
            for attribute_name in result.keys():
                data[attribute_name].append(result[attribute_name])

        repo = pd.DataFrame(data)
        repo.to_csv(filename)

    def _process_match(self, match):

        home_stats = self.counters[match['home']]
        away_stats = self.counters[match['away']]

        entry = {}

        for key in home_stats.keys():
            entry[key + '_home'] = home_stats[key]

        for key in away_stats.keys():
            entry[key + '_away'] = away_stats[key]

        self._update_counters(match)
        return entry

    def _winner(self, match):
        if int(match['score_home']) > int(match['score_away']):
            return 'home'
        if int(match['score_home']) < int(match['score_away']):
            return 'away'
        if int(match['score_home']) == int(match['score_away']):
            return 'none'

    def _add_to_results(self, entry):
        self.results.append(entry)

    def _add_to_counter(self, team, key, qty):
        self.counters[team][key] += int(qty)

    def _set_counter(self, team, key, value):
        self.counters[team][key] = value

    def reindex(self):
        '''
        Añade un atributo fecha en formato yyyymmgg para poder ordenar
        :return:
        '''

        self.logger.debug('Reindexing...')
        self.mongo_wrapper.drop_collection(self.collection)

        for match in self.mongo_wrapper.get_collection('primera_results').find():
            tmp = match['day'].split('-')
            if len(tmp[0]) == 1:
                tmp[0]= '0' + tmp[0]
            converted = tmp[2] + tmp[1] + tmp[0]
            match['day_yyyymmgg'] = converted

            self.mongo_wrapper.write_dictionary(self.collection, match)

        self.logger.debug('Done')

    def _init_counters(self):
        self.counters = {}
        self.teams_recent_history = {}

        for team in self._collection().find().distinct("home"):
            self.counters[team] = self.counter_template.copy()
            self.teams_recent_history[team] = {'goals': []}


    def _collection(self):
        return self.mongo_wrapper.get_collection(self.collection)

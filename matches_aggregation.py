from scraping.aggregations.MatchesFactsAggregator import MatchesFactsAggregator
from scraping.laliga import utils

csv_filename = './dataset_partidos_past.csv'
if csv_filename == '':
    print('Setea la ruta completa del fichero CSV')
    exit()

aggregator = MatchesFactsAggregator()
seasons = utils.build_seasons('primera', 1928)
seasons.append('primera/2017-18')

for season in seasons:
    aggregator.process_matches_played(season)
aggregator.write_data_csv(csv_filename)

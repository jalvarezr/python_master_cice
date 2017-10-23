from scraping.marca import ResultsCurrentYearScraper as scraper
from scraping.core import stdout_logger

print('Downloading marca web data')
logger = stdout_logger.Logger(2)
s = scraper.ResultsCurrentYearScraper(logger)
s.scrape()
print('Done')


print('Downloading as web classifications data')
from scraping.as_web import AsClassificationScraper

scraper = AsClassificationScraper.AsClassificationScraper()
scraper.scrape_page()
print('Done')

print('Reindexing...')
from scraping.aggregations.MatchesFactsAggregator import MatchesFactsAggregator
aggregator = MatchesFactsAggregator()
aggregator.reindex()
print('Done')


print('Merging...')
from scraping.aggregations.ResultsMerger import ResultsMerger
merger = ResultsMerger()
results = merger.merge()
merger.save(results)
print('Done')




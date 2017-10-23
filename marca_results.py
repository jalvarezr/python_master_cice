from scraping.marca import ResultsCurrentYearScraper as scraper
from scraping.core import stdout_logger



print('START')

logger = stdout_logger.Logger(2)
s = scraper.ResultsCurrentYearScraper(logger)
s.scrape()

print('END')

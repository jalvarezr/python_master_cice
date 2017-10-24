from scraping.marca import ResultsCurrentYearScraper as scraper
from scraping.core import stdout_logger



print('START')

s = scraper.ResultsCurrentYearScraper()
s.scrape()

print('END')

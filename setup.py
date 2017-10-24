#Marca
from scraping.marca import ResultsCurrentYearScraper as scraper
s = scraper.ResultsCurrentYearScraper()
s.scrape()

#As
from scraping.as_web import AsClassificationScraper

scraper = AsClassificationScraper.AsClassificationScraper()
scraper.scrape_page()

#Merge
from scraping.aggregations.ResultsMerger import ResultsMerger
merger = ResultsMerger()
results = merger.merge()
merger.save(results)





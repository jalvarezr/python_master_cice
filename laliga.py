from scraping.laliga import utils
from scraping.laliga import SeasonScraper
from multiprocessing import Pool



# las temporadas a procesar

#seasons = utils.build_seasons('primera', 2001) + utils.build_seasons('segunda', 2001)
seasons = utils.build_seasons('primera', 1928)
#seasons = build_league('primera', 1928) + build_league('segunda', 1993)

def scrape_season(season):

    scraper = SeasonScraper.SeasonScraper()
    scraper.scrape_page(season)


print('BEGIN PROCESS')

#writer = utils.create_mongo_writer()
#writer.reset()

if __name__ == '__main__':


    p = Pool(5)
    p.map(scrape_season, seasons)

print('END PROCESS')

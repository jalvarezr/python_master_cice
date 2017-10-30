from scraping.futbolFantasy_jugadores.PlayersScraper import PlayersScraper
from scraping.core import stdout_logger


print('START')

j = PlayersScraper()
j.scrape_page('http://www.futbolfantasy.com')

print('END')

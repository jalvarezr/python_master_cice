import re
from bs4 import BeautifulSoup
from scraping.laliga import utils
from scraping.laliga import PopUpScraper
from scraping.core import scrape_request
from scraping.core.stdout_logger import Logger

class SeasonScraper:

    def __init__(self):
        self.sender = scrape_request.Sender()
        self.sender.set_delay(2)
        self.sender.set_debug_level(2)
        self.logger = Logger(2)

    def scrape_page(self, season):

        splitted = season.split('/')
        league = splitted[0]
        page_content = self.do_request(season)
        writer = utils.create_mongo_writer(league)

        if page_content:
            html = BeautifulSoup(page_content, 'html.parser')
            days = html.find_all('div', {'class': 'jornada-calendario-historico'})

            matches_results = []

            for day in days[:]:

                table_title = day.find('div', {'class': 'nombre_jornada'})
                day_str = self.extract_day(table_title.contents[0])
                day_num = self.extract_daynum(table_title.contents[0])

                tables = day.find_all('table', {'class': 'tabla_jornada_calendario_historico'})

                for table in tables[:]:
                    rows = table.find_all('tr', {'onclick': re.compile('^abrir_partido')})

                    for row in rows[:]:
                        js_params = self.extract_popup_win_js_params(row['onclick'])

                        if js_params != False:

                            match_id = js_params['temporada'] + '_' + js_params['jornada'] + '_' + \
                                       js_params['equipo'] + '_' + js_params['competicion']

                            cell = row.find('td')
                            content_to_process = str(cell.contents[0])
                            txt = self.extract_result(content_to_process)

                            matches_results.append(
                                {
                                    'season': season,
                                    'day': day_str,
                                    'day_num': day_num,
                                    'home': str.strip(txt.group(1)),
                                    'away': str.strip(txt.group(3)),
                                    'score_home': txt.group(2),
                                    'score_away': txt.group(4),
                                    'match_id': match_id
                                }
                            )
                            popup_scraper = PopUpScraper.PopUpScraper(match_id, writer)
                            popup_scraper.scrape_popup(js_params)

            writer.write_dictionaries_list('results', matches_results)

    def do_request(self, path):
        sender = self.sender
        url = 'http://www.laliga.es/estadisticas-historicas/calendario/' + path + '/'

        return sender.get(url, {})

    def extract_result(self, content_to_process):
        '''
        :param content_to_process: Cosas como '<span>RCD Mallorca: <b>1</b><br>Real Madrid: <b>2</b></span>'
        :return: array con 4 elementos para nombre del equipo y goles
        '''
        content_to_process = content_to_process.replace(":", "")
        cell_pattern_str = '<span>(.+?)<b>(.+?)</b><br/>(.+?)<b>(.+?)</b></span>'
        return re.search(cell_pattern_str, content_to_process)

    def extract_daynum(self, content_to_process):
        '''
        :param content_to_process: Cosas como 'Jornada: 02 - 26/08/2016'
        :return: fecha en formato dd-mm-yyyy
        '''
        cell_pattern_str = '(\d+)'
        parsed = re.search(cell_pattern_str, content_to_process)
        return str.strip(parsed.group(1))

    def extract_day(self, content_to_process):
        cell_pattern_str = '(\d+/\d+/\d+)'
        parsed = re.search(cell_pattern_str, content_to_process)
        return str.strip(str.replace(parsed.group(1), "/", "-"))

    def extract_popup_win_js_params(self, function_call_str):
        '''
        Saca los parámetros a pasar al JS que obtiene los popups con los detalles de un partido.
        ej. 'abrir_partido(115,37,"barcelona",1)'
        '''
        pattern = 'abrir_partido\((.+?),(.+?),"(.+?)",(.+?)\)'
        parsed = re.search(pattern, function_call_str)

        if parsed is None:
            self.logger.error(400, 'Error in extract_popup_win_js_params ' + function_call_str)
            return False
        else:
            return {'temporada': parsed.group(1),
                    'jornada': parsed.group(2),
                    'equipo': parsed.group(3),
                    'competicion': parsed.group(4)}


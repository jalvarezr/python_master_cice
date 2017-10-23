import datetime

from bs4 import BeautifulSoup
from scraping.core import prefixed_mongo_wrapper
from scraping.core import scrape_request
from scraping.core.stdout_logger import Logger

class AsClassificationScraper:

    def __init__(self):
        self.writer_obj = False
        self.collection = 'as_classifications_data'
        self.started = datetime.datetime.now().isoformat()
        self.logger = Logger(2)

    def writer(self):

        if not self.writer_obj:
            self.writer_obj = prefixed_mongo_wrapper.PrefixedMongoWrapper(self.collection)

        return self.writer_obj

    def scrape_page(self):
        sender = scrape_request.Sender()
        sender.set_debug_level(2)
        raw_html = sender.get('https://resultados.as.com/resultados/futbol/primera/clasificacion/', {})
        self.process_page(raw_html)

    def process_page(self, raw_html):
        html = BeautifulSoup(raw_html, 'html.parser')

        main_table = html.find('table', {'class', 'tabla-datos table-hover'})
        header = self.process_header(main_table)

        data_table = main_table.find('tbody')
        for row in data_table.find_all('tr'):
            self.process_row(row, header)

    def process_header(self, main_table):
        result = []
        head = main_table.find_all('th', {'scope': 'col'})

        for column in head:
            result.append(column.getText().replace('.', ''))

        return result

    def process_row(self, row, header):

        team_content = row.find('th').find('span', {'class': 'nombre-equipo'}).getText()
        result = {'process_id': self.started, 'team': team_content, 'total': {}, 'home': {}, 'away': {}}
        cell_counter = 1
        for cell in row.find_all('td'):

            if cell_counter <= 7:
                result['total'][header[cell_counter]] = cell.getText()
            else:
                if cell_counter > 7 and cell_counter <= 14:
                    result['home'][header[cell_counter]] = cell.getText()
                else:
                    result['away'][header[cell_counter]] = cell.getText()
            cell_counter = cell_counter + 1
        self.writer().write_dictionary('classification', result)
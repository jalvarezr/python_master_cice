import requests
import time

from scraping.core.stdout_logger import Logger

class Sender:
    '''
    Clase para en manejo de peticiones REST
    '''

    def __init__(self, logger = Logger()):
        self.logger = logger
        self.delay = 0

    def set_delay(self, delay):
        self.delay = delay

    def get(self, url, params):
        '''
        wrapper para requests.get
        :param url:
        :param params:
        :return:
        '''
        response = requests.get(url, params)

        msg = 'GET ' + url + ' [' + str(response.status_code) + ']'
        self.logger.debug(msg)
        if response.status_code == 200:
            result = response.text
            if result == '':
                self.logger.error(500, 'Empty response')
        else:
            self.logger.error(response.status_code, 'Unable to connect to ' + url)
            result = ''

        time.sleep(self.delay)

        return result

    def post(self, url, params):
        '''
        wrapper para requests.post
        :param url:
        :param params:
        :return:
        '''

        response = requests.post(url, params)
        msg = 'POST ' + url + ' with ' + str(params) +  ' [' + str(response.status_code) + ']'
        self.logger.debug(msg)
        if response.status_code == 200:
            result = response.text
            if result == '':
                self.logger.error(500, 'Empty response')

        else:
            self.logger.error(response.status_code, 'Unable to connect to ' + url)
            result = ''

        time.sleep(self.delay)

        return result
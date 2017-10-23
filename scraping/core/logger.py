import logging


class Logger:

    def __init__(self, modulo):
        self.logger = logging.getLogger(modulo)
        hdlr = logging.FileHandler(modulo + '.log')
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        hdlr.setFormatter(formatter)
        self.logger.addHandler(hdlr)
        self.logger.setLevel(logging.DEBUG)

    def debug(self, msg):
        '''
        Para mensajes informativos: "sending request", etc
        :param msg:
        :return:
        '''

        self.logger.debug(msg)


    def log(self, code, msg):
        '''
        para lo que queramos trazar para hacer seguimiento: URLs procesadas, etc
        :param code:
        :param msg:
        :return:
        '''
        self.logger.info(code + ' ' + msg)



    def error(self, code, msg):
        '''
        Utilizar este método cuando se produzca algún error grave: que altera al resultado final
        :param code:
        :param msg:
        :return:
        '''
        self.logger.error(str(code) + ' ' + msg)

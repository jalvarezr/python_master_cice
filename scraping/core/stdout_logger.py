class Logger:

    def __init__(self, detail_level = 0):
        '''
        :param detail_level: 0 Error, 1 Log, 2 Debug
        '''
        self.detail_level = detail_level

    def set_detail_level(self, level):
        self.detail_level = level

    def debug(self, msg):
        if self.detail_level == 2:
            print('[DEBUG] ' + msg)


    def log(self, code, msg):
        if self.detail_level >= 1:
            print('[LOG] ' + str(code) + ' ' + msg)

    def error(self, code, msg):
        print('[ERROR] ' + str(code) + ' ' + msg)


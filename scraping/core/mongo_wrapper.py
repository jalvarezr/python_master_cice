import pymongo


class MongoWriter:
    '''
    Wrapper para operaciones comunes sobre una base de datos MongoDB
    '''

    def __init__(self):

        self.host = 'localhost'
        self.port = 27017
        self.mongo_database_name = 'scraping'

        self.mongo_client_obj = pymongo.MongoClient(self.host, self.port)

    def write_dictionaries_list(self, collection_name, dictionaries):
        '''
        Añade a una collección un array de diccionarios
        :param collection_name: Nombre de la collección
        :param dictionaries: Lista de diccionarios
        :return:
        '''
        if len(dictionaries) > 0:
            collection = self.database().get_collection(collection_name)
            collection.insert_many(dictionaries)

    def write_dictionary(self, collection_name, dictionary):
        '''
        Añade un a tupla
        :param collection_name: Nombre de la collección
        :param dictionary: Diccionario con los datos de la tupla
        :return:
        '''
        collection = self.database().get_collection(collection_name)
        collection.insert_one(dictionary)

    def database(self):
        return self.client().get_database(self.mongo_database_name)

    def drop_collection(self, collection):
        self.database().drop_collection(collection)

    def client(self):

        if not self.mongo_client_obj:
            self.mongo_client_obj = pymongo.MongoClient(self.host, self.port)

        return self.mongo_client_obj

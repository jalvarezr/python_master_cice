from scraping.core import mongo_wrapper

class PrefixedMongoWrapper:
    '''
    Otro wrapper
    Pone un prefijo antes de cada nombre de collección que utilice
    '''
    def __init__(self, prefix):

        self.prefix = prefix
        self.writer = mongo_wrapper.MongoWriter()

    def _create_collection_name(self, name):
        return self.prefix + '_' + name

    def write_dictionaries_list(self, collection_name, dictionaries):

        collection_name = self._create_collection_name(collection_name)
        self.writer.write_dictionaries_list(collection_name, dictionaries)

    def write_dictionary(self, collection_name, dictionary):

        collection_name = self._create_collection_name(collection_name)
        self.writer.write_dictionary(collection_name, dictionary)

    def drop_collection(self, collection_name):
        collection_name = self._create_collection_name(collection_name)
        self.writer.drop_collection(collection_name)

    def database(self):
        return self.writer.database()

    def get_collection(self, name):
        return self.database().get_collection(self._create_collection_name(name))

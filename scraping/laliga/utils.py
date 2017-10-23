from scraping.core import prefixed_mongo_wrapper

def create_mongo_writer(league = ''):
    '''
    :param league: 'primera' vs 'segunda'
    :return:
    '''
    prefix = 'laliga_web'
    if league != '':
        #ej. laliga_web_primera
        prefix = prefix + '_' + league
    return prefixed_mongo_wrapper.PrefixedMongoWrapper(prefix)


def build_seasons(league, year_start):
    '''
    Genera un array con los posibles identificadores de liga/temporada
    ej. 'segunda/2015-16'.
    Sirve para generar URLs como http://www.laliga.es/estadisticas-historicas/calendario/segunda/2015-16
    :param league: 'primera' vs. 'segunda'
    :param year_start: año de comienzo
    :return:
    '''
    result = []
    for year in range(year_start, 2017):

        year_to_postfix = str((int(str(year)[2:]) + 1))
        if len(year_to_postfix) == 1:
            year_to_postfix = '0' + year_to_postfix

        season = league + '/' + str(year) + '-' + year_to_postfix
        result.append(season)
    return result

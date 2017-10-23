"""
	OBTENCION DE ALINEACIONES POSIBLES PARA PARTIDOS DE LA LIGA SANTANDER DE LA WEB DE FUTBOL FANTASY
	Javier Alvarez 9/10/17
	
	En este script se instancia un objeto de la clase ""getAlineacion" y se ejecuta el metodo principal "getDataAlineaciones"
	PRERREQUISITO: es necesario que exista en el servidor local de Mongo la coleccion "colAlineaciones" en la BD "alineaciones"
	
	PENDIENTE: extraer posicion en la que juega el jugador
	PENDIENTE: Usar multihilo. Es el codigo es seguro para multihilo???
	PENDIENTE: usar un fochero de configuracion
"""

from scraping.twitter_futbolfantasy.getAlineacion import getAlineacion

a = getAlineacion()

if a.getDataAlineaciones() == 0:
    print("Ejecucion terminada con exito. Consulte la BD alineaciones")
else:
    print("Ha ocurrido un error recuperando los datos. Consulte el fichero de log.")
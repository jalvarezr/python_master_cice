# CLASE getAlineacion: OBTENCION DE ALINEACIONES POSIBLES PARA PARTIDOS DE LA LIGA SANTANDER DE LA WEB DE FUTBOL FANTASY
# Javier Alvarez 9/10/17
# Las alineaciones se publican en la web de futbol fantasy, previo aviso en Twitter. El objetivo de 
# este script es "peinar" la cuenta de Twitter de futbol_fantasy para extraer los avisos de la publicacion 
# de alineaciones y luego descargarlas a traves de web scrapping. Por ultimo se guardan los datos en una coleccion
# "colAlineaciones" de la BD de mongo "alineaciones"

# Principales funciones en esta clase:

# getTwitterData() : busca entre los Tweets de la cuenta futbol_fantasy los que mencionen la publicacion de 
# una posible alineacion en su web

# getWebData() : Extrae alineaciones posibles a traves de scrapping en la parte de cada equipo dentro de la 
# web de futbol fantasy: http://www.futbolfantasy.com/laliga/equipos/<equipo>

# getDataAlineaciones: ejecuta las dos funciones anteriores y guarda los resultados a la BD Mongo "alineaciones"

# PENDIENTE: extraer posicion en la que juega el jugador
# PENDIENTE: el codigo es seguro para multihilo???

import requests
import os
import urllib.request
import oauth2 as oauth
import json

from bs4 import BeautifulSoup
from multiprocessing import Pool
from scraping.core.logger import Logger
from scraping.core.mongo_wrapper import MongoWriter

class getAlineacion():
	"""
	
	"""
	
	def __init__(self):
		"""comentario
			PENDIENTE: leer valores de fichero de configuracion
		"""
		
		self.credencialesTwitter = { "consumer_key"    : "*******************",
				"consumer_secret" : "***************************",
				"token_key"       : "******************************",
				"token_secret"    : "*********************************" }
				 
		self.cadenaABuscar = "Posible XI"
		self.numMaxTweets = '900'
		self.nombreFicheroIndice = 'maxId.dat'
		self.l = Logger('getAlineaciones')
		self.dbWriter = MongoWriter()

	def augment(self, url, parameters) :
		'''
		Construct, sign, and open a twitter request
		using the hard-coded credentials above.
		'''
		
		consumer = oauth.Consumer(key=self.credencialesTwitter['consumer_key'], secret=self.credencialesTwitter['consumer_secret'])
		token    = oauth.Token(key=self.credencialesTwitter['token_key'], secret=self.credencialesTwitter['token_secret'])

		signature_method_hmac_sha1 = oauth.SignatureMethod_HMAC_SHA1()

		request = oauth.Request.from_consumer_and_token(consumer,
			token, http_method='GET', http_url=url, parameters=parameters)

		request.sign_request(signature_method_hmac_sha1, consumer, token)
		headers = request.to_header()
		return request.to_url()
		
	def getMaxTweetId(self) :
		"""
		"""
		
		fname = self.nombreFicheroIndice
		
		try:
			with open(fname) as f:
				content = f.readlines()
		except Exception:
			return 0
		content = [x.strip() for x in content]
		if len(content) > 0:
			return int(content[0])
		else:
			return 0
		
	def saveMaxTweetId(self, maxId) :
		"""
		"""
		
		fileMaxId = open(self.nombreFicheroIndice,'w')  
		fileMaxId.write(str(maxId))
		fileMaxId.close()     
		
	def getTwitterData(self):  
		# Descarga tweets de la cuenta futbol_fantasy y filtra segun aparezca la cadena "Posible XI" al inicio del texto.
		# Saca del tweet el nombre del equipo y la URL con la alineacion posible y lo guarda en un diccionario
		
		diccURLsEquipos = {}
		equipo=""
		URLAlineacion = ""
		maxIdEnCurso=0
		
		# se recupera el id de tweet mas alto de la anterior ejecucion para pedir a partir de ese
		# si es cero se marca un numero maximo de tweets a descargar
		maxId = self.getMaxTweetId()    
		diccParametros = {'screen_name': 'futbol_fantasy'}
		if maxId == 0:
			diccParametros['count'] = self.numMaxTweets
		else:
			diccParametros['since_id'] = str(maxId)
		
		self.l.debug('getAlineacion.getTwitterData: Parametros llamada Twitter: ' + str(diccParametros))
		
		url = self.augment('https://api.twitter.com/1.1/statuses/user_timeline.json', diccParametros)    
		connection = urllib.request.urlopen(url)
		data = connection.read()
		js = json.loads(data.decode("utf-8"))     # lo codificamos a utf-8
		
		self.l.debug('getAlineacion.getTwitterData: Tweets recuperados ANTES DE FILTRAR: ' + str(len(js)))
		
		# se busca la cadena "Posible XI" al inicio del texto del tweet y se extrae el hashtag con 
		# el nombre del equipo y la URL con la alineacion posible
		for tweets in js[:]:
			if tweets['id'] > maxIdEnCurso:
				maxIdEnCurso = tweets['id']
			s=tweets['text']        
			if s[:len(self.cadenaABuscar)] == self.cadenaABuscar:
				for palabraTweet in s.split():
					if palabraTweet.startswith("#"):
						equipo = palabraTweet
					if palabraTweet.startswith("http"):
						URLAlineacion = palabraTweet
					if len(equipo) > 0 and len(URLAlineacion) > 0:
						diccURLsEquipos[equipo] = URLAlineacion                
		
		# se guarda el id de tweet mas alto para pedir a partir de ese en la proxima ejecucion
		self.saveMaxTweetId(maxIdEnCurso)
		
		return diccURLsEquipos

	def getWebData(self, equipo, url) :
		# Extrae alineaciones posibles a traves de scrapping en la parte de cada equipo dentro de la web de futbol fantasy
		# http://www.futbolfantasy.com/laliga/equipos/<equipo>.
		# Recibe URL de la web del equipo y el nombre del equipo

		response = requests.get(url)

		msg = 'GET ' + url + ' [' + str(response.status_code) + ']'
		if response.status_code == 200:
			result = response.text.encode('utf-8')
		else:
			result = ''
		
		retorno = []
		if result:
			html = BeautifulSoup(result, 'html.parser')        
			numJornada = html.find_all('h2', {'class': 'main title'})
			numJornadaTxt = numJornada[0].text.strip()

			# La funcion solo deberia bajar alineaciones posibles. Para diferenciar entre una alineacion posible y una 
			# alineacion confirmada en la web http://www.futbolfantasy.com/laliga/equipos/<equipo> he comprobado que la 
			# clase de la seccion con los datos del equipo es "mod alineacion_wrapper" para las 
			# alineaciones posibles y "mod alineacion_wrapper past" para las definitivas. Esto cambia la visibilidad de
			# las capas con el titulo, dejando solo una visible, "Posible alineación" o "Alineación confirmada" 
			esAlineacionPosible  = html.find_all('section', {'class': 'mod alineacion_wrapper'})
			esAlineacionConfirmada  = html.find_all('section', {'class': 'mod alineacion_wrapper past'})
			tipoAlineacion = "Alineacion Posible" if len(esAlineacionPosible) > 0 else "Alineacion Confirmada"
			
			#PENDIENTE: SACAR LA POSICION DE CADA JUGADOR
			jugadores = html.find_all('a', {'class': 'juggador'})
			for jugador in jugadores:
				retorno.append({'equipo' : equipo, 'jornada' : numJornadaTxt, 'alineacion' : tipoAlineacion, 'jugadores' : jugador.text.strip()})
		return retorno
			
	def getDataAlineaciones(self):
		"""
			Funcion principal. Ejecuta la busqueda de Tweets y luego usa las URLs obtenidas para descargar las alineaciones 
			via web Scrapping.
		"""
		dicAlineaciones = []
		dicTweets = {}
		
		# primer paso, se buscan los tweets que anuncian los posibles 11
		self.l.log('getAlineacion.getDataAlineaciones', ' -------------- Proceso Iniciado -------------')
		try:
			dicTweets = self.getTwitterData()
		except Exception:
			self.l.error('getAlineacion.getDataAlineaciones', 'Error al recuperar datos de Twitter: ')
			return 1
		self.l.log('getAlineacion.getDataAlineaciones', 'Tweets recuperados:' + str(len(dicTweets)))
		
		# se realiza web scrapping sobre las paginas de los equipos con posibles 11 actualizados
		for k, v in dicTweets.items():
			if len(k) > 0 and len(v) > 0:
				self.l.debug("getAlineacion.getDataAlineaciones: Procesando web del equipo: " + str(k) + str(v))
				try:
					diccTemp = self.getWebData(k, v)
				except Exception:
					self.l.error('getAlineacion.getDataAlineaciones', 'Error al recuperar datos de la web: ')
					return 2
				dicAlineaciones.append(diccTemp)
						
		# por ultimo, se guardan los resultados a base de datos Mongo "alineaciones"
		for alineacion in dicAlineaciones:
			try:
				self.dbWriter.write_dictionaries_list('colAlineaciones', alineacion)
			except pymongo.errors.BulkWriteError as e:
				self.l.error('getAlineacion.getDataAlineaciones', 'Error al escribir a BD: ' + e)
				return 3
		
		self.l.debug('getAlineacion.getDataAlineaciones: Alineacion:' + str(dicAlineaciones))
		self.l.log('getAlineacion.getDataAlineaciones', ' -------------- Proceso Finalizado -------------')
		
		return 0
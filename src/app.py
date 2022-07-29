import descarga
import procesa
from reg_logs import log
from fuentes import *
from decouple import config
#print(config('PgSQL_HOST'))

log.info('Descargando archivos fuente')
descarga.descargar_csv(URL_BIBLIOTECAS, 'bibliotecas')
descarga.descargar_csv(URL_MUSEOS, 'museos')
descarga.descargar_csv(URL_CINES, 'cines')

log.info('Conectando con base de datos y subiendo tablas')
descarga.cargar_tablas()

log.info('Procesando datos y creando tablas')
procesa.procesar_datos()

log.info('Ejecución finalizada con éxito')
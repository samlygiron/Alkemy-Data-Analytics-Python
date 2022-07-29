import os 
import requests
import decouple
import pandas as pd
from reg_logs import log
from fuentes import *
from datetime import date
from decouple import config
from sqlalchemy import create_engine, Date, String, Integer

# Guarda en la variable hoy la fecha de descarga de los archivos
hoy = date.today()
mes=hoy.strftime('%m')

def descargar_csv(url, categoria):
    '''
    Descarga y guarda la informacion del archivo .csv
    '''
    try:
        # Configuracion de la carpeta destino
        #carpeta = categoria + '/' + str(hoy.year) + '-' + str[hoy.month]
        carpeta = categoria + '/' + str(hoy.year) + '-' + mes
        # En caso no exista, crear:
        if not os.path.exists(carpeta):
                os.makedirs(carpeta)
        
        # Genera la ruta y nombre del archivo a procesar
        filename = carpeta + '/' + categoria + '-' + str(hoy.day) + '-' + mes + '-' + str(hoy.year) + '.csv'

        # Descargo el archivo y lo guardo en esa ruta
        req = requests.get(url)
        url_content = req.content
        csv_file = open(filename, 'wb')
        csv_file.write(url_content)
        csv_file.close()

        log.info('Descarga de los archivos realizada')
        
    except Exception as e:
        log.error('No se pudo completar la descarga de los archivos')
        print(f'Error al realizar la descarga: {e}')

if __name__ == "__main__":
    descargar_csv(URL_BIBLIOTECAS, 'bibliotecas')
    descargar_csv(URL_MUSEOS, 'museos')
    descargar_csv(URL_CINES, 'cines')


# Conexion de base de datos PostgreSQL usando sqlalchemy
engine = create_engine('postgresql+psycopg2://' + config('PgSQL_USER') + ':' + config('PgSQL_PASSWORD') + '@' + config('PgSQL_HOST') + ':' + config('PgSQL_PORT') + '/'+ config('PgSQL_NAME'))

def cargar_tablas():
    '''
    Conecta con la base de datos y la actualiza con las tablas creadas
    '''
    try:
        # Obtengo la tabla conjunta
        df_conjunta = pd.read_csv('df_conjunto.csv')

        # Subo la tabla a la base de datos
        df_conjunta.to_sql('datos_conjuntos', con=engine, if_exists='replace', index=False, dtype={
            'cod_localidad':String, 'id_provincia':String, 'id_departamento':String, 'categoria':String,
            'provincia':String, 'localidad':String, 'nombre':String, 'domicilio':String, 'código postal':String,
            'número de teléfono':String, 'mail':String, 'web':String, 'fecha_carga':Date})

        # Obtengo la tabla de cantidad de registros
        df_registros = pd.read_csv('df_cantidad_registros.csv')

        # Subo la tabla a la base de datos
        df_registros.to_sql('cantidad_registros', con=engine, if_exists='replace', index=False, dtype={
            'categoría':String, 'total por categoría':Integer, 'fuente':String, 'total por fuente':Integer, 
            'provincia':String, 'categorías por provincia':Integer, 'fecha_carga':Date})

        # Obtengo la tabla de salas de cine
        df_cines = pd.read_csv('df_cines.csv')

        # Subo la tabla a la base de datos
        df_cines.to_sql('info_cines', con=engine, if_exists='replace', index=False, dtype={
            'provincia':String, 'pantallas':Integer, 'butacas':Integer, 'espacios_INCAA':Integer, 'fecha_carga':Date})
        
        log.info('Se actualizó la base de datos con éxito')

    except Exception as e:
        log.error('No se pudieron subir las tablas')
        print(f'Error al subir las tablas: {e}')

if __name__ == '__main__':
    cargar_tablas()




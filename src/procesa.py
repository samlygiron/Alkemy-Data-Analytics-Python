import pandas as pd 
import numpy as np
import logging
import decouple
from reg_logs import log
from datetime import date 
from fuentes import *

# Almaceno la fecha de descarga de los archivos
hoy = date.today()
mes=hoy.strftime('%m')

def procesar_datos():
    '''
    Normaliza toda la información de museos, salas de cine y bibliotecas,  creando una tabla total con todos los datos conjuntos.
    cod_localidad
            o id_provincia
            o id_departamento
            o categoría
            o provincia
            o localidad
            o nombre
            o domicilio
            o código postal
            o número de teléfono
            o mail
            o web
    Procesa la tabla de datos conjuntos y crea una tabla con la cantidad de registros por categoría, fuente y provincia-categoría.
    Procesa la información de cines y crea una tabla con la cantidad de butacas, pantallas y espacios INCAA por provincia.
    '''
    try:
        # Cargo los datos del csv de bibliotecas
        carpeta = 'bibliotecas' + '/' + str(hoy.year) + '-' + mes
        df_bibliotecas = pd.read_csv(carpeta + '/' + 'bibliotecas' + '-' + str(hoy.day) + '-' + mes + '-' + str(hoy.year) + '.csv', encoding='UTF-8')

        # Filtro las columas de la tabla total
        df_bibliotecas = df_bibliotecas[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría', 'Provincia', 'Localidad',
                                        'Nombre', 'Domicilio', 'CP', 'Teléfono', 'Mail', 'Web', 'Fuente']]

        # Renombro las columnas
        df_bibliotecas.rename(
                            columns={'Cod_Loc':'cod_localidad', 'IdProvincia':'id_provincia', 'IdDepartamento':'id_departamento', 'Categoría':'categoría',
                            'Provincia':'provincia', 'Localidad':'localidad', 'Nombre':'nombre', 'Domicilio':'domicilio', 'CP':'código postal',
                            'Teléfono':'número de teléfono', 'Mail':'mail', 'Web':'web', 'Fuente':'fuente'}, inplace=True)

        # Cargo los datos del csv de salas de cine
        carpeta = 'cines' + '/' + str(hoy.year) + '-' + mes
        df_cines = pd.read_csv(carpeta + '/' + 'cines' + '-' + str(hoy.day) + '-' + mes + '-' + str(hoy.year) + '.csv', encoding='UTF-8')

        # Filtro las columas de la tabla total
        df_salas_de_cine = df_cines[['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']].copy()
        # Renombro las columnas
        df_salas_de_cine.rename(columns={'Provincia':'provincia', 'Pantallas':'pantallas', 'Butacas':'butacas'}, inplace=True)

        # En la columna "espacio_INCAA" mayormente hay valores nulos o con el valor sí. 
        # Los valores nulos pasan a 0
        df_salas_de_cine.replace('0', np.nan, inplace=True)

        # Proceso los datos para generar la tercera tabla
        df_salas_de_cine = df_salas_de_cine.groupby('provincia').aggregate({'pantallas': 'sum', 'butacas':'sum', 'espacio_INCAA':'count'}).reset_index()

        #Agrego la fecha de carga
        df_salas_de_cine = df_salas_de_cine.assign(fecha_carga=hoy)

        # Lo guardo en un nuevo csv
        df_salas_de_cine.to_csv('df_cines.csv',index=False, encoding='UTF-8')

        # Me quedo con las columnas de interés
        df_cines = df_cines[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría', 'Provincia', 'Localidad',
                                        'Nombre', 'Dirección', 'CP', 'Teléfono', 'Mail', 'Web', 'Fuente']]

        # Renombro las columnas
        df_cines.rename(
                        columns={'Cod_Loc':'cod_localidad', 'IdProvincia':'id_provincia', 'IdDepartamento':'id_departamento', 'Categoría':'categoría',
                        'Provincia':'provincia', 'Localidad':'localidad', 'Nombre':'nombre', 'Dirección':'domicilio', 'CP':'código postal',
                        'Teléfono':'número de teléfono', 'Mail':'mail', 'Web':'web', 'Fuente':'fuente'}, inplace=True)


        # Cargo los datos del csv de museos
        carpeta = 'museos' + '/' + str(hoy.year) + '-' + mes
        df_museos = pd.read_csv(carpeta + '/' + 'museos' + '-' + str(hoy.day) + '-' +mes + '-' + str(hoy.year) + '.csv', encoding='UTF-8')

        # Me quedo con las columnas de interés
        df_museos = df_museos[['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'categoria', 'provincia', 'localidad',
                                'nombre', 'direccion', 'CP', 'telefono', 'Mail', 'Web', 'fuente']]

        # Renombro las columnas
        df_museos.rename(
                        columns={'Cod_Loc':'cod_localidad', 'IdProvincia':'id_provincia', 'IdDepartamento':'id_departamento', 'dirección':'domicilio',
                        'categoria':'categoría', 'CP':'código postal','telefono':'número de teléfono', 'Mail':'mail', 'Web':'web'}, inplace=True)

        # Creo un dataframe con la información conjunta que acabo de procesar
        df_total = pd.concat([df_bibliotecas, df_cines, df_museos])

        # Agrego la columna correspondiente a la fecha de carga 
        df_total = df_total.assign(fecha_carga=hoy)

        # Reemplazo los valores sin datos ("s/d") por null
        df_total = df_total.replace('s/d', np.nan)

        # Proceso los datos conjuntos para generar la segunda tabla  
        df_categoria = df_total.value_counts('categoría').reset_index(name='total por categoría')
        df_fuente = df_total.value_counts('fuente').reset_index(name='total por fuente')
        df_provincia = df_total.value_counts(['categoría', 'provincia']).reset_index(name='total por provincia y categoría')
        df_provincia.insert(0, 'provincia y categoría', df_provincia['provincia'] + "/" + df_provincia['categoría'])
        df_provincia.drop(['categoría', 'provincia'], axis=1, inplace=True)

        # Genero la segunda tabla 
        df_registros = pd.concat([df_categoria, df_fuente, df_provincia], axis=1)

        # Agrego la fecha de carga
        df_registros = df_registros.assign(fecha_carga=hoy)

        # Guardo en un nuevo csv 
        df_registros.to_csv('df_cantidad_registros.csv', index=False, encoding='UTF-8')

        # Elimino la columna fuente ya que solo la agregué para generar la segunda tabla
        df_total.drop('fuente', axis=1, inplace=True)

        # Guardo en un nuevo csv los datos conjuntos
        df_total.to_csv('df_conjunto.csv', index=False, encoding='UTF-8')

        log.info('Los datos fueron procesados con éxito')

    except Exception as e:
        log.error('No pudieron procesarse correctamente los datos')
        print(f'Error al procesar los datos: {e}')

if __name__ == '__main__':
    procesar_datos()
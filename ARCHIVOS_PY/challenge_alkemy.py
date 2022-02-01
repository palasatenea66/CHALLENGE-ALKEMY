#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 23:42:16 2022

@author: AnahiRomo

"""
# importación de librerías necesarias
import requests
import pandas as pd
import os
import logging
import locale
from config import config
from datetime import date, datetime
from sqlalchemy import create_engine
from unidecode import unidecode



def descargar_fuentes(enlace, nombre_fuente):
    '''Descarga archivo solicitado desde un sitio web en directorio actual.
    Parámetros: enlace (url) y nombre_fuente (archivo a descargar)'''
    archivo = enlace + nombre_fuente
    fuente = requests.get(archivo, allow_redirects = True) # llamo al sitio...
    open(nombre_fuente, 'wb').write(fuente.content)   # y descargo archivo

    return nombre_fuente


def ordenar_fuentes(nombre_archivo):
    '''Cambia nombre al archivo fuente descargado y lo reubica en el directorio
    que: categoría/mes-año/categoria-dia-mes-año.csv
    Parámetro: nombre del archivo a reubicar'''

    #locale.setlocale(locale.LC_ALL, ('es_ES.utf8')) # cambio el idioma a ES
    fecha = date.today()    # fecha de descarga ('hoy') como objeto datetime
    hoy = datetime.strftime(fecha, '%d-%m-%Y')   # lo paso a cadena
    anio = datetime.strftime(fecha, '%Y') # recupero como str: año...
    mes_letras = datetime.strftime(fecha, '%B') #  y mes en letras en español
    categoria = nombre_archivo[:-4]    # preparo el nombre del subdirectorio...
    extension = nombre_archivo[-4:]    # y el nombre del archivo
    subdir = anio + '-' + mes_letras
    nombre_nuevo = categoria + '-' + hoy + extension
    # nombre completo con ruta al directorio
    fuente_ordenada = os.path.join(categoria, subdir, nombre_nuevo)
    # creo dir y subdir y evito que dé error si ya existe con exist_ok = True
    os.makedirs(os.path.join(categoria, subdir), exist_ok = True)
    os.rename(nombre_archivo, fuente_ordenada)  # renombro el archivo de fuente

    return fuente_ordenada


def cargar_csv2pandas(archivo):
    ''' Carga el archivo en un dataframe de Pandas y asigna tipo de datos a 
    algunas columnas comunes a todos los archivos. 
    Crea una nueva columna con la fecha de carga.'''
    df = pd.read_csv(archivo)           # cargo el dataframe
        
    # paso nombre de columnas a minúsculas y elimino tildes y 'ñ'
    columnas = df.columns.values.to_list()
    nuevas = []
    for columna in columnas:
        nueva_columna = unidecode(columna.lower())
        nuevas.append(nueva_columna)
    df.columns = nuevas
    print(df.columns)

    df.fillna(0, inplace = True)        # reemplazo NaN x 0's 
    
    # asigno tipos de datos a algunas de las columnas que comparten nombre en 
    # todos los archivos
    df[['cod_loc', 'idprovincia', 'iddepartamento', 
        'telefono']].astype(dtype = 'int')
    df[['categoria', 'provincia', 'direccion', 'nombre', 'mail', 'web',
        'localidad', 'piso', 'tipolatitudlongitud', 'cp',
        'fuente']].astype(dtype = 'str')
    df[['latitud', 'longitud']].astype(dtype = 'float')

    # recupero la fecha a partir del nombre del archivo     
    fecha_carga = archivo[-14:-4]
    # agrego col de fecha de carga
    df['fecha_carga'] = fecha_carga

    return df
  
def cambiar_tel(df):
    ''' Agrega al 'df' una columna  'num_tel' con el número de teléfono
     y su código de área con formato "str".'''

    # combino 'cod_area' + 'telefono' en una columna: 'num_tel'
    df['cod_area'] = df['cod_area'].astype(dtype = 'str')
    df['telefono'] = df['telefono'].astype(dtype = 'str')    
    df['num_tel'] = df['cod_area'] + '-' + df['telefono']

    return df

def cargar_museo_en_df(museo_archivo):
    ''' Convierte el archivo con info de 'museos' a un dataframe, renombra y
    agrega columnas necesarias para normalizar con los demás archivos de cine y
    bibliotecas. También agrega una columna con la fecha de carga.
    Parámetro: nombre de archivo de museos'''

    museo_df = cargar_csv2pandas(museo_archivo) # cargo el dataframe

    # renombro algunas columnas para normalizar nombres de todos los archivos
    museo_df.rename(columns = {'info_adicional': 'informacion adicional'}, 
                    inplace = True)

    # agrego columnas faltantes en su pos para normalizar con 'cine' y 'biblio'
    museo_df.insert(7, 'departamento', 0)
    museo_df.insert(25, 'tipo_gestion', 0)
    museo_df.insert(26, 'pantallas', 0)
    museo_df.insert(27, 'butacas', 0)
    museo_df.insert(28, 'espacio_incaa', 0)
    museo_df.insert(29, 'ano_actualizacion', 0)

    # asigno tipos de datos a las columnas que faltaban
    museo_df[['informacion adicional','departamento','observaciones','direccion',
    'subcategoria', 'tipo_gestion', 'jurisdiccion']].astype(dtype = 'str')
    museo_df[['pantallas', 'butacas', 'espacio_incaa', 'cod_area', 'idsinca',
              'ano_actualizacion', 'año_inauguracion']].astype(dtype = 'int')

    museo_df = cambiar_tel(museo_df)
    
    return museo_df


def cargar_cine_en_df(cine_archivo):
    ''' Convierte el archivo con info de 'cines' a un dataframe, renombra y
    agrega columnas necesarias para normalizar con los demás archivos de museos
    y bibliotecas. También agrega una columna con la fecha de carga.
    Parámetro: nombre de archivo de cine'''

    cine_df = cargar_csv2pandas(cine_archivo) # cargo el dataframe
    

    cine_df['espacio_incaa'] = cine_df.espacio_incaa.map({'si': 1, 'SI': 1})    

    # agrego columnas p/ normalizar con 'museo' y 'biblioteca' en posic fijas
    cine_df.insert(5, 'subcategoria', 0)
    cine_df.insert(22,'jurisdiccion', 0)
    cine_df.insert(23, 'ano_inauguracion', 0)
    cine_df.insert(24, 'idsinca', 0)

    # asigno tipo de datos a las columnas no definidas antes        
    cine_df[['informacion adicional','departamento','observaciones','direccion',
         'subcategoria', 'tipo_gestion', 'jurisdiccion']].astype(dtype = 'str')
    cine_df[['pantallas', 'butacas', 'espacio_incaa', 'cod_area', 'idsinca',
              'ano_actualizacion', 'año_inauguracion']].astype(dtype = 'int')
    
    cine_df = cambiar_tel(cine_df)
     
    return cine_df


def cargar_biblio_en_df(biblio_archivo):
    ''' Convierte el archivo con info de 'bibliotecas' a un dataframe, renombra
    y agrega columnas necesarias para normalizar con los demás archivos de cine
    y museos. También agrega una columna con la fecha de carga.
    Parámetro: nombre de archivo de bibliotecas'''

    biblio_df = cargar_csv2pandas(biblio_archivo) # cargo el dataframe
    biblio_df.fillna(0, inplace = True)     # reemplazo NaN x 0's

    # renombro columnas para normalizar con 'museo' y 'cine'
    biblio_df.rename(columns= {'observacion': 'observaciones',
                               'domicilio': 'direccion',
                               'cod_tel': 'cod_area',
                               'ano_inicio': 'ano_inauguracion'},
                     inplace = True)

    # agrego columnas para normalizar con 'museo' y 'cine'
    biblio_df.insert(22,'jurisdiccion', 0)
    biblio_df.insert(24, 'idsinca', 0)
    biblio_df.insert(26, 'pantallas', 0)
    biblio_df.insert(27, 'butacas', 0)
    biblio_df.insert(28, 'espacio_incaa', 0)
    
    # asigno tipo de datos a las columnas no definidas antes        
    biblio_df[['informacion adicional','departamento','observaciones','direccion',
         'subcategoria', 'tipo_gestion', 'jurisdiccion']].astype(dtype = 'str')
    biblio_df[['pantallas', 'butacas', 'espacio_incaa', 'cod_area', 'idsinca',
              'ano_actualizacion', 'año_inauguracion']].astype(dtype = 'int')
    
    biblio_df = cambiar_tel(biblio_df)

    return biblio_df


def unir_df(df1, df2, df3):
    ''' Concatena los tres dataframes de museos, cines y bibliotecas, para
    disponer de todos los datos en un único dataframe
    Parámetros: nombre de los dataframe a concatenar'''

    # unimos los dataframes de los archivo-fuente,
    unico_df = pd.concat([df1, df2, df3])
    # reemplazo los valores NaN (ausentes en los archivos descargados) por 0's
    unico_df.fillna(0, inplace = True)
    # ordena los índices de las filas desde el ppio de corrido, sin repetir
    unico_df.index = range(unico_df.shape[0])

    return unico_df


def preparar_info2tabla1(df_completo):
    '''Crea un nuevo dataframe seleccionando las columnas necesarias para
    popular la tabla1 (identifica cada espacio cultural) de la base de datos.
    Parámetro: nombre de dataframe con datos conjuntos'''

    # selecciono las columnas para la Tabla1
    col_selec = ['cod_loc', 'idprovincia', 'iddepartamento', 'categoria',
                 'provincia', 'localidad', 'nombre', 'direccion', 'cp',
                 'num_tel', 'mail', 'web','fecha_carga']
    df_tabla1 = df_completo[col_selec].copy() # copio para no sobreescribir df

    return df_tabla1


def agrupar_dataframe(nombre_df, lista_agrupar):
    '''Agrupa un dataframe según algunas de sus características y devuelve 
    uno nuevo'''

    # agrupo según las características elegidas
    df_agrupado = nombre_df.groupby(lista_agrupar)
    # colapso todas las columnas en una tomando count() de 'Nombre'
    df_colapsado = df_agrupado.nombre.count()
    # x colapsar las columnas, se convierte en Serie, que hay que pasar a df
    # 'lista_agrupar' se toma como índice, se reinterpreta c/ reset_index
    total_agrupado = (df_colapsado.to_frame()).reset_index()
    # renombro la cantidad
    total_agrupado.rename(columns = {'nombre': 'cantidad total'})
    # agrego la fecha
    total_agrupado['fecha_carga'] = nombre_df['fecha_carga']

    return total_agrupado


def preparar_info2tabla2(df_completo):
    ''' Crea un nuevo dataframe seleccionando las columnas necesarias para
    popular la tabla2 (categoría, fuente y provincia) de la base de datos.
    Parámetro: nombre de dataframe con datos conjuntos'''

    # columnas 'Categoría', 'Provincia', 'Fuente', 'fecha_carga'

    # empiezo con agrupar x Categoría
    totXcategoria = agrupar_dataframe(df_completo, ['categoria'])
    print(totXcategoria)

    # agrupamos por Fuente     
    totXfuente = agrupar_dataframe(df_completo, ['fuente'])
    print(totXfuente)

    # ahora agrupamos por Provincia y Categoría
    totXcat_prov = agrupar_dataframe(df_completo, ['provincia', 'categoría'])
    print(totXcat_prov)

    df_tabla2 = df_completo.copy()   # sacar y modificar la tabla

    return df_tabla2


def preparar_info2tabla3(df_completo):
    '''Crea un nuevo dataframe seleccionando las columnas necesarias para
    popular la tabla3 (sobre cines) de la base de datos.
    Parámetro: nombre de dataframe con datos conjuntos '''

    # selecciono las columnas para la Tabla3
    col_selec = ['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA']

    # selecciono sólo las 'Salas de cine'
    df_tab3 = df_completo[df_completo['Categoría'] ==
                            'Salas de cine'].copy()
    # creo df nuevo c/ las col necesarias, agrupado por provincia, la suma de
    # los valores agrupados y con
    df_tabla3 = (df_tab3[col_selec].groupby('Provincia').sum()).reset_index()
    df_tabla3['fecha_carga'] = df_completo['fecha_carga']
    print(df_tabla3)

    return df_tabla3


def conectar_y_popular_db(df_tabla1, df_tabla2, df_tabla3):
    ''' Genera una conexión al motor de PostgreSQL usando sessionmaker y
    scoped_session de SQLalchemy, para popular las tablas previamente creadas
    corriendo 'crear_tablas_postgres.py' mediante df.to_sql.'''

    # desde 'config.ini': 'postgresql://postgres:pepe@localhost:5432/postgres'
    param = config(archivo = 'config.ini', seccion = 'postgresql')
    uno = param['user']
    dos = param['password']
    tres = param['host']
    cuatro = param['port']
    cinco = param['database']
    base_datos = f"postgresql://{uno}:{dos}@{tres}:{cuatro}/{cinco}"
    motor_db = create_engine(base_datos)

    # populo las tablas: si hay información previa, la reemplazo
    # usar 'with xxx as xxx:' garantiza q la conexión será cerrada al terminar
    # la inyección de datos
    with motor_db.begin() as conn:
        df_tabla1.to_sql('tabla1', conn, if_exists = 'replace', index = False)
        df_tabla2.to_sql('tabla2', conn, if_exists = 'replace', index = False)
        df_tabla3.to_sql('tabla3_cines', conn, if_exists = 'replace',
                         index = False)

    return


def el_anillo_unico_para_gobernarlos_a_todos():
    ''' Organiza todo el pipeline de descarga y procesamiento de los archivos
    fuente hasta la población de las tablas Postgres.
    Extrae los urls de archivos fuente y parámetros para conectarse al motor de
    Postgres del archivo de configuración "config.ini"'''

    # levanto los urls de los archivos fuentes desde 'config.ini'
    urls = config(archivo = 'config.ini', seccion = 'url')
    url_museo = urls.get('url_museo')
    url_cine = urls.get('url_cine')
    url_biblio = urls.get('url_biblio')

    # descargo archivos fte de museo, cine, biblio
    fuente_museo = descargar_fuentes(url_museo, 'museo.csv')
    fuente_cine = descargar_fuentes(url_cine, 'cine.cvs')
    fuente_biblio = descargar_fuentes(url_biblio, 'biblioteca_popular.csv')

    # renombro y ordeno en subdirectorios los archivos fuente
    museo = ordenar_fuentes(fuente_museo)
    cine = ordenar_fuentes(fuente_cine)
    biblio = ordenar_fuentes(fuente_biblio)

    # armo y normalizo dataframes con los archivos ya ordenados
    df_museo = cargar_museo_en_df(museo)
    df_cine = cargar_cine_en_df(cine)
    df_biblio = cargar_biblio_en_df(biblio)

    # uno los 3 dataframes en un único df
    df_datos_cjtos = unir_df(df_museo, df_cine, df_biblio)

    # preparo df 's con la info para popular las tablas de la BD
    tabla1_df = preparar_info2tabla1(df_datos_cjtos)
    tabla2_df = preparar_info2tabla2(df_datos_cjtos)
    tabla3_df = preparar_info2tabla3(df_datos_cjtos)

    # populo las tablas ya creadas con 'crear_tablas_postgres.py' c/ df.to_sql
    conectar_y_popular_db(tabla1_df, tabla2_df, tabla3_df)




if __name__ == '__main__':

    el_anillo_unico_para_gobernarlos_a_todos()

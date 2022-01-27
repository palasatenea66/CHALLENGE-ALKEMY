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

    locale.setlocale(locale.LC_ALL, ('es_ES', 'UTF-8')) # cambio el idioma a ES
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


def cargar_museo_en_df(museo_archivo):
    ''' Convierte el archivo con info de 'museos' a un dataframe, renombra y
    agrega columnas necesarias para normalizar con los demás archivos de cine y
    bibliotecas. También agrega una columna con la fecha de carga.
    Parámetro: nombre de archivo de museos'''

    museo_df = pd.read_csv(museo_archivo) # cargo el dataframe
    museo_df.fillna(0, inplace = True)  # reemplazo NaN x 0's

    fecha_carga = museo_archivo[-14:-4]
    # renombro algunas columnas para que coincidan con 'cine' y 'biblioteca'
    museo_df.rename(columns = {'categoria': 'Categoría',
                               'Info_adicional': 'Información adicional',
                               'provincia': 'Provincia',
                               'localidad': 'Localidad',
                               'nombre': 'Nombre',
                               'direccion': 'Dirección',
                               'piso': 'Piso',
                               'telefono': 'Tel',
                               'fuente': 'Fuente'}, inplace = True)


    # agrego columnas faltantes en su pos para normalizar con 'cine' y 'biblio'
    museo_df.insert(7, 'Departamento', 0)
    museo_df.insert(25, 'tipo_gestion', 0)
    museo_df.insert(26, 'Pantallas', 0)
    museo_df.insert(27, 'Butacas', 0)
    museo_df.insert(28, 'espacio_INCAA', 0)
    museo_df.insert(29, 'año_actualizacion', 0)

    # agrego col de fecha descarga
    museo_df['fecha_carga'] = fecha_carga

    # combino 'cod_area' + 'Tel' en una columna: 'Num_tel'
    museo_df['cod_area'] = (museo_df['cod_area'].astype(int)).astype(str)
    museo_df['Tel'] = museo_df['Tel'].astype(str)
    museo_df['Num_tel'] = museo_df['cod_area'] + '-' + museo_df['Tel']


    return museo_df


def cargar_cine_en_df(cine_archivo):
    ''' Convierte el archivo con info de 'cines' a un dataframe, renombra y
    agrega columnas necesarias para normalizar con los demás archivos de museos
    y bibliotecas. También agrega una columna con la fecha de carga.
    Parámetro: nombre de archivo de cine'''

    cine_df = pd.read_csv(cine_archivo) # cargo el dataframe
    cine_df.fillna(0, inplace = True)   # reemplazo NaN x 0's

    fecha_carga = cine_archivo[-14:-4]

    cine_df.rename(columns = {'Teléfono': 'Tel'}, inplace = True)

    # agrego columnas p/ normalizar con 'museo' y 'biblioteca' en posic fijas
    cine_df.insert(5, 'subcategoria', 0)
    cine_df.insert(22,'jurisdiccion', 0)
    cine_df.insert(23, 'año_inauguracion', 0)
    cine_df.insert(24, 'IDSInCA', 0)
    cine_df.insert(30, 'fecha_carga', fecha_carga)

    # combino 'cod_area' + 'Tel' en una columna: 'Num_tel'
    cine_df['cod_area'] = cine_df['cod_area'].astype(str)
    cine_df['Tel'] = cine_df['Tel'].astype(str)
    cine_df['Num_tel'] = cine_df['cod_area'] + '-' + cine_df['Tel']


    return cine_df


def cargar_biblio_en_df(biblio_archivo):
    ''' Convierte el archivo con info de 'bibliotecas' a un dataframe, renombra
    y agrega columnas necesarias para normalizar con los demás archivos de cine
    y museos. También agrega una columna con la fecha de carga.
    Parámetro: nombre de archivo de bibliotecas'''

    biblio_df = pd.read_csv(biblio_archivo) # cargo el dataframe
    biblio_df.fillna(0, inplace = True)     # reemplazo NaN x 0's


    fecha_carga = biblio_archivo[-14:-4]
    # renombro columnas para normalizar con 'museo' y 'cine'
    biblio_df.rename(columns= {'Observacion': 'Observaciones',
                               'Subcategoria': 'subcategoria',
                               'Domicilio': 'Dirección',
                               'Cod_tel': 'cod_area',
                               'Teléfono': 'Tel',
                               'año_inicio': 'año_inauguracion',
                               'Tipo_gestion': 'tipo_gestion',
                               'Año_actualizacion': 'año_actualizacion'},
                     inplace = True)

    # agrego columnas para normalizar con 'museo' y 'cine'
    biblio_df.insert(22,'jurisdiccion', 0)
    biblio_df.insert(24, 'IDSInCA', 0)
    biblio_df.insert(26, 'Pantallas', 0)
    biblio_df.insert(27, 'Butacas', 0)
    biblio_df.insert(28, 'espacio_INCAA', 0)
    biblio_df.insert(30, 'fecha_carga', fecha_carga)

    # combino 'cod_area' + 'Tel' en una columna: 'Num_tel'
    biblio_df['cod_area'] = biblio_df['cod_area'].astype(str)
    biblio_df['Tel'] = biblio_df['Tel'].astype(str)
    biblio_df['Num_tel'] = biblio_df['cod_area'] + '-' + biblio_df['Tel']


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
    col_selec = ['Cod_Loc', 'IdProvincia', 'IdDepartamento', 'Categoría',
                 'Provincia', 'Localidad', 'Nombre', 'Dirección', 'CP',
                 'Num_tel', 'Mail', 'Web','fecha_carga']
    df_tabla1 = df_completo[col_selec].copy() # copio para no sobreescribir df

    return df_tabla1


def preparar_info2tabla2(df_completo):
    ''' Crea un nuevo dataframe seleccionando las columnas necesarias para
    popular la tabla2 (categoría, fuente y provincia) de la base de datos.
    Parámetro: nombre de dataframe con datos conjuntos'''

    fecha = date.today()    # fecha de descarga ('hoy') como objeto datetime
    hoy = datetime.strftime(fecha, '%d-%m-%Y') # ahora, como str
    # columnas 'Categoría', 'Provincia', 'Fuente', 'fecha_carga'
    # copio para no sobreescribir df original
    df_categoria = (df_completo.copy()).groupby('Categoría')
    cant_tot_categoria = df_categoria.Nombre.count()
    cant_tot_categoria.to_frame()
    cant_tot_categoria['fecha_carga'] = hoy

    print(cant_tot_categoria)
    print(cant_tot_categoria.shape)

    df_fuente = (df_completo.copy()).groupby('Fuente')
    cant_tot_fuente = df_fuente.Nombre.count()
    cant_tot_fuente.to_frame()
    cant_tot_fuente['fecha_carga'] = hoy

    print(cant_tot_fuente)
    print(cant_tot_fuente.shape)

    df_prov_cat = (df_completo.copy()).groupby(['Categoría','Provincia'])
    cant_tot_prov_cat = df_prov_cat.Nombre.count()
    cant_tot_prov_cat.to_frame()
    cant_tot_prov_cat['fecha_carga'] = hoy

    print(cant_tot_prov_cat)
    print(cant_tot_prov_cat.shape)

    df_tabla2 = df_completo.copy()   # sacar y modificar la tabla


    return df_tabla2


def preparar_info2tabla3(df_completo):
    '''Crea un nuevo dataframe seleccionando las columnas necesarias para
    popular la tabla3 (sobre cines) de la base de datos.
    Parámetro: nombre de dataframe con datos conjuntos '''

    # selecciono las columnas para la Tabla3
    col_selec = ['Provincia', 'Pantallas', 'Butacas', 'espacio_INCAA',
                 'fecha_carga']

    # selecciono sólo las 'Salas de cine' y creo df nuevo c/ las col necesarias
    df_tab3 = df_completo[df_completo['Categoría'] ==
                            'Salas de cine'][col_selec].copy()
    df_tabla3 = df_tab3.count()

    print(df_tabla3)
    print(df_tabla3.shape)

    # indico que estas columnas tienen números enteros
    #df_tabla3['Pantallas'] =  df_tabla3['Pantallas'].astype(int)
    #df_tabla3['Butacas'] =  df_tabla3['Butacas'].astype(int)


    return df_tabla3

def conectar_y_popular_db(df_tabla1, df_tabla2, df_tabla3):
    ''' Genera una conexión al motor de PostgreSQL usando sessionmaker y
    scoped_session de SQLalchemy, para popular las tablas previamente creadas
    corriendo 'crear_tablas_postgres.py' mediante df.to_sql.'''

    # viene de 'config.ini': 'postgresql://postgres:pepe@localhost:5432/postgres'
    param = config(seccion = 'postgresql')
    uno = param['user']
    dos = param['password']
    tres = param['host']
    cuatro = param['port']
    cinco = param['database']
    base_datos = f"postgresql://{uno}:{dos}@{tres}:{cuatro}/{cinco}"
    motor_db = create_engine(base_datos)

    # populo las tablas, si hay información previa, la reemplazo
    # usar 'with xxx as xxx: garantiza que la conexión será cerrada al terminar
    # la inyección de datos
    with motor_db.begin() as con:
        df_tabla1.to_sql('tabla1', con, if_exists = 'replace', index = False)
        df_tabla2.to_sql('tabla2', con, if_exists = 'replace', index = False)
        df_tabla3.to_sql('tabla3_cines', con, if_exists = 'replace',
                         index = False)

    return


def fn_ppal():
    ''' Organiza todo el pipeline de descarga y procesamiento de los archivos
    fuente hasta la población de las tablas Postgres.
    Extrae los urls y parámetros para conectarse al motor de Postgres del
    archivo de configuración "config.ini"'''

    # levanto los urls de los archivos fuentes desde 'config.ini'
    urls = config(seccion = 'url')
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
    print(df_datos_cjtos['Web'])

    # preparo df 's con la info para popular las tablas de la BD
    tabla1_df = preparar_info2tabla1(df_datos_cjtos)
    tabla2_df = preparar_info2tabla2(df_datos_cjtos)
    tabla3_df = preparar_info2tabla3(df_datos_cjtos)

    # populo las tablas ya creadas con 'crear_tablas_postgres.py' c/ df.to_sql
    conectar_y_popular_db(tabla1_df, tabla2_df, tabla3_df)




if __name__ == '__main__':

    fn_ppal()

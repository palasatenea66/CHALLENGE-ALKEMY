#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jan 23 02:16:52 2022

@author: AnahiRomo
"""
from config import config
import psycopg2, psycopg2.extras


def ejecutar_sql():
    """ Conexión al servidor de pases de datos PostgreSQL y creación de las
    tablas en la base de datos"""

    conexion = None
    tablas = ['crear_tabla1.sql','crear_tabla2.sql', 'crear_tabla3_cines.sql']
    try:
        # Lectura de los parámetros de conexion
        params = config(archivo = 'config.ini', seccion = 'postgresql')

        # Conexion al servidor de PostgreSQL
        print('Conectando a la base de datos PostgreSQL...')
        conexion = psycopg2.connect(**params)

        # creación del cursor
        cur = conexion.cursor()

        # Crear las tablas en BD de PostgreSQL
        for tabla in tablas:
            with open(tabla, 'r') as ordenes_sql:
                instrucciones = ordenes_sql.read()
                cur.execute(instrucciones)

        # Confirmar el envío de las instrucciones
        conexion.commit()

        # Cierre de la comunicación con PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conexion is not None:
            conexion.close()
            print('Conexión finalizada!')


if __name__ == '__main__':

    ejecutar_sql()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Jan 24 04:49:11 2022

@author: AnahiRomo
"""

from configparser import ConfigParser

def config(archivo ='config.ini', seccion):
    '''Lee parámetros de un archivo de configuración 'config.ini', para su uso
    posterior '''

    # Crear el parser y leer el archivo
    parser = ConfigParser()
    parser.read(archivo)

    # Obtener la sección de conexión a la base de datos
    db = {}
    
    if parser.has_section(seccion):
        params = parser.items(seccion)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Secccion {0} no encontrada en el archivo {1}'.
                        format(seccion, archivo))

    return db

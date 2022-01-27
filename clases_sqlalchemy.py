#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 25 18:51:31 2022

@author: AnahiRomo

"""
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base


def modelo_tabla1():
    ''' '''
    Base = declarative_base()

    class Tabla1(Base):
        __tablename__ = 'tabla1'

        cod_loc = Column(Integer)
        id_prov = Column(Integer)
        id_dep = Column(Integer)
        categoria = Column(String)
        provincia = Column(String)
        localidad = Column(String)
        nombre = Column(String, primary_key = True)
        domicilio = Column(String)
        cp = Column(Integer)
        num_tel = Column(String)
        mail = Column(String)
        web = Column(String)
        fecha_carga = Column(String, nullable = False)

        def __init__(self, cod_loc, id_prov, id_dep, categoria, provincia,
                     localidad, nombre, domicilio, cp, num_tel, mail, web,
                     fecha_carga):
            self.cod_loc = cod_loc
            self.id_prov = id_prov
            self.id_dep = id_dep
            self.categoria = categoria
            self.provincia = provincia
            self.localidad = localidad
            self.nombre = nombre
            self.domicilio = domicilio
            self.cp = cp
            self.num_tel = num_tel
            self.mail = mail
            self.web = web
            self.fecha_carga = fecha_carga

        def __repr__(self):
            return f'Tabla1({self.provincia}, {self.nombre})'

        def __str__(self):
            return self.nombre


def modelo_tabla2():
    ''' '''

    Base = declarative_base()

    class Tabla2(Base):
        __tablename__ = 'tabla2'

        reg_tot_categoria = Column(Integer)
        reg_tot_fuente = Column(Integer)
        reg_tot_cat_prov = Column(Integer)
        fecha_carga = Column(String)

        def __init__(self, reg_tot_categoria, reg_tot_fuente, reg_tot_cat_prov,
                     fecha_carga):
            self.reg_tot_categoria = reg_tot_categoria
            self.reg_tot_fuente = reg_tot_fuente
            self.reg_tot_cat_prov = reg_tot_cat_prov
            self.fecha_carga = fecha_carga

        def __repr__(self):
            return f'Tabla2(Registros totales, {self.fecha_carga})'

        def __str_(self):
            return self.fecha_carga


def modelo_tabla3():
    ''' '''

    Base = declarative_base()

    class Tabla3_cines(Base):
        __tablename__ = 'tabla3_cines'

        provincia = Column(String)
        pantallas = Column(Integer)
        butacas = Column(Integer)
        esp_incaa = Column(String)
        fecha_carga = Column(String)

        def __init__(self, provincia, pantallas, butacas, esp_incaa,
                     fecha_carga):
            self.provincia = provincia
            self.pantallas = pantallas
            self.butacas = butacas
            self.esp_incaa = esp_incaa
            self.fecha_carga = fecha_carga

        def __repr__(self):
            return f'Tabla3_cines({self.provincia})'

        def __str__(self):
            return self.provincia









if __name__ == '__main__':



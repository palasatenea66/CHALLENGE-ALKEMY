# challenge alkemy
# autor: Jonatan Montenegro
# %%
# importamos librerias a utilizar
import os
import requests
import datetime
import pandas as pd
import csv
from datetime import datetime
import time

#%%
def create_folder(path):
    try:
        file_path = os.getcwd() + path
        print(file_path)
        # Determinar si el directorio ya existe
        if not os.path.exists(file_path):
            # El directorio no existe, crea la operación
            # Utilice el método os.makedirs () para crear directorios de niveles múltiples
            os.makedirs(file_path)
            print("Directorio creado con éxito:" + file_path)
        else:
            print("¡El directorio ya existe!")
    except BaseException as msg:
        print("Error al crear el nuevo directorio:" + msg)
# %%


def fetch_venues(csv_url, type):
    ''' la funcion invoca a la funcion create folder y crea
        
    '''

    with requests.Session() as s:
        
        (time.time())
        date = time.strftime("%Y-%B", time.localtime(time.time()))
        folder = f"./{type}/{date}"
        create_folder(folder)
        
        download = s.get(csv_url)
        decoded_content = download.content.decode('latin-1')
        cr = csv.reader(decoded_content.splitlines(), delimiter=',')
        df = pd.DataFrame(cr)       
        df.to_csv(f"{folder}/{type}-{datetime.today().strftime('%d-%m-%Y')}.csv")
        


# %%
if __name__ == '__main__':

    cines_url = ('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/392ce1a8-ef11-4776-b280-6f1c7fae16ae/download/cine.csv')
    museo_url = ('https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/4207def0-2ff7-41d5-9095-d42ae8207a5d/download/museos.csv')
    bibliotecas_url = (
        'https://datos.cultura.gob.ar/dataset/37305de4-3cce-4d4b-9d9a-fec3ca61d09f/resource/01c6c048-dbeb-44e0-8efa-6944f73715d7/download/biblioteca_popular.csv')

    fetch_venues(cines_url, "cines")
    fetch_venues(museo_url, 'museos')
    fetch_venues(bibliotecas_url, 'bibliotecas')


# %%
import pandas as pd
directorio='./bibliotecas/2021-December'
archivo='bibliotecas-09-12-2021.csv'
fname= os.path.join(directorio,archivo)
df = pd.read_csv(fname,encoding='latin-1')
# %%

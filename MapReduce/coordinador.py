
#permite trabajar con expresiones regulares estas son patrones de búsqueda que 
#se utilizan para encontrar coincidencias dentro de cadenas de texto
import re

#proporciona funciones para interactuar con el sistema operativo, 
#manipulación de archivos y directorios, obtener información sobre el SO
import os 


import multiprocessing
from concurrent.futures import ThreadPoolExecutor


import multiprocessing
from pathlib import Path
import random
from threading import Thread, current_thread
import time

from map import map_execution

nodes_cores = multiprocessing.cpu_count()
print(f"CORES -> {nodes_cores} \t")
nodes_map = int(nodes_cores*0.6 if (nodes_cores*0.60 >= 4) else 4)
nodes_reducer = int(nodes_cores*0.3 if (nodes_cores*0.3 >= 2) else 2)

print(f"MAP cores -> {nodes_map} \t"
      f"REDUCER cores -> {nodes_reducer} \t")

#Funcion para limpiar los caracteres del texto
def clean_file(file):
    
    with open(file, "r", encoding='utf-8') as f:
        #reemplaza con 1 espacio:

        #caracteres no alfanumericos ni numeros
        text = re.sub(r'[^a-zA-Z\s]', ' ', f.read())
        #tabs
        text = re.sub(r'\t', ' ', text)
        #varios espacios
        text = re.sub(r'\s+', ' ', text)
        

    #guardar texto formateado en newLittleWomen.txt
    with open("ficheros/newLittleWomen.txt", "w", encoding='utf-8') as f:
        f.write(text)

#Funcion para dividir el fichero en chunks del mismo tamano
def split_file(clean_file, chunks_folder, num_chunks):
    total_size = os.path.getsize(clean_file)
    chunk_size = total_size // num_chunks

    with open(clean_file, "r", encoding='utf-8') as f:
        chunk_number = 1
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break

            output_file = os.path.join(chunks_folder, f'part_{chunk_number}.txt')
            with open(output_file, "w", encoding='utf-8') as output_f:
                output_f.write(chunk)

            chunk_number += 1



if __name__ == "__main__":

    # paso 1 Limpieza del archivo
    
    filePath = "ficheros/LittleWomenShortVersion.txt"
    chunksPath = 'ficheros/chunks'
    cleanFilePath = "ficheros/newLittleWomen.txt"
    numChunks = 20
    clean_file(filePath)

    # Paso 2 Dividir el archivo en chunks mas pequenios

    stateCoordinator = input("Inducir un error al nodo coordinador? S/N: ").upper()

    if stateCoordinator == 'N':
        print(f'DIVISION DE FICHERO: {cleanFilePath} EN CHUNKS')
        split_file(cleanFilePath,chunksPath, numChunks)
        print("Chunks Creados")

        #paso 3 Creacion de las instancias de map con los fragmentos que van a utilizar
    
        print("----- Mapeo -----")
        
        #Decision Error Nodo Map
        state_map = input('Inducir un error al nodo map? S/N: ').upper()
        
        #lectura de archivos chunks dentro de la carpeta ficheros
        chunks_list = os.listdir("ficheros/chunks")
        chunks_list_size = len(chunks_list)
        print(f"Chunks que entran en el Map -> {chunks_list_size} \t")
        
        if state_map == 'S':
            #Bandera de parar nodo
            error_map = True
            #Elejir al azar el nodo a parar
            map_random = random.randint(0, nodes_map)
        else:
            error_map = False
        
        #Inicio Nodos Map
        with ThreadPoolExecutor(max_workers=nodes_map) as executor:
            cnt = 0
            
            
            for chunk in chunks_list:
                
                #Caso de Bandera parar nodo True
                if error_map and cnt==map_random:
                    print(f"Error nodo map {current_thread().name} \t")
                    executor.submit(map_execution, chunk)
                    cnt = cnt + 1
                    continue
                #Inicio Proceso Map
                executor.submit(map_execution, chunk)
                cnt = cnt + 1

            

        print("ALL MAP NODES HAVE FINISHED MAPPING THEIR ASSIGNED CHUNKS")
        
        executor.shutdown()
        

    
    #paso 4 Combiner (por ver)
    #paso 5 Reduce
                
    else:
        print('ERROR! SE NECESITA EJECUTAR DE NUEVO EL PROGRAMA')





#permite trabajar con expresiones regulares estas son patrones de bÃºsqueda que 
#se utilizan para encontrar coincidencias dentro de cadenas de texto
import re

#proporciona funciones para interactuar con el sistema operativo, 
#manipulaciÃ³n de archivos y directorios, obtener informaciÃ³n sobre el SO
import os 


import multiprocessing


import random
import threading 
from threading import current_thread


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



def ejecutar_hilos(lista, ErrorNodoMap):
    id = current_thread().name
    print(f"Nodo Paralelo {id} inicio")
    #list_size = len(lista)
    
    # Crear un lock para sincronizar el acceso a la lista
    lock = threading.Lock()
    if (ErrorNodoMap==False):
        print(f"Creacion Nodos en {id}")
        hilo1 = threading.Thread(target=map_execution, args=(lista,lock,False))
        hilo2 = threading.Thread(target=map_execution, args=(lista,lock,False))
    
        hilo1.start()
        hilo2.start()
    
        hilo1.join()
        hilo2.join()
    else:
        print(f"Creacion Nodos en {id}")
        hilo1 = threading.Thread(target=map_execution, args=(lista,lock,True))
        hilo2 = threading.Thread(target=map_execution, args=(lista,lock,False))
        print(f"Detener Hilo 1 en {id}")
        hilo1.start()
        hilo2.start()
        
        hilo1.join()
        hilo2.join()
        
    print(f"Nodo Paralelo {id} Finalizo, Esperando a los demas")


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
        for f in os.listdir("ficheros/mapped_chunks"):
            os.remove(os.path.join("ficheros/mapped_chunks", f))
        #print(f"Lista Chunks que entran en el Map -> {chunks_list} \t")

        
        if state_map == 'S':
            #Elegir al azar el nodo a parar
            map_random = random.randint(0, nodes_map)
            
            #Bandera de parar nodo
            if map_random<=1:
                print(f"El Error ocurrira en el hilo de grupo 1  \t")
                error_map1 = True
                error_map2 = False
            else:
                print(f"El Error ocurrira en el hilo de grupo 2  \t")
                error_map1 = False
                error_map2 = True
            #Elegir al azar el nodo a parar
            map_random = random.randint(0, nodes_map)
        else:
            error_map1 = False
            error_map2 = False
        
        print(f"Inicio Paralelismo \t")
        #Inicio Nodos Map
        hilo_grupo1 = threading.Thread(target=ejecutar_hilos, args=(chunks_list[(int(chunks_list_size/2)):],error_map1))
        hilo_grupo2 = threading.Thread(target=ejecutar_hilos, args=(chunks_list[:(int(chunks_list_size/2))],error_map2))
        
        hilo_grupo1.start()
        hilo_grupo2.start()
        
        hilo_grupo1.join()
        hilo_grupo2.join()
        
        print("Todos los hilos han finalizado.")
        
        

    
    #paso 4 Combiner (por ver)
    #paso 5 Reduce
                
    else:
        print('ERROR! SE NECESITA EJECUTAR DE NUEVO EL PROGRAMA')




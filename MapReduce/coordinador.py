
#permite trabajar con expresiones regulares estas son patrones de búsqueda que 
#se utilizan para encontrar coincidencias dentro de cadenas de texto
import re

#proporciona funciones para interactuar con el sistema operativo, 
#manipulación de archivos y directorios, obtener información sobre el SO
import os 

#Funcion para limpiar los caracteres del texto
def clean_file(file):
    
    with open(file, "r", encoding='utf-8') as f:
        #reemplaza con 1 espacio:

        #caracteres no alfanumericos
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
    CleanFilePath = "ficheros/newLittleWomen.txt"
    numChunks = 20
    clean_file(filePath)

    # Paso 2 Dividir el archivo en chunks más pequeños

    stateCoordinator = input("Inducir un error al nodo coordinador? S/N: ").upper()

    if stateCoordinator == 'N':
        print(f'DIVISION DE FICHERO: {CleanFilePath} EN CHUNKS')
        split_file(CleanFilePath,chunksPath, numChunks)
        print("Chunks Creados")

    #paso 3 Creacion de las instancias de map con los fragmentos que van a utilizar
    #paso 4 Combiner (por ver)
    #paso 5 Reduce
                
    else:
        print('ERROR! SE NECESITA EJECUTAR DE NUEVO EL PROGRAMA')



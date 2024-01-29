from threading import Thread, current_thread


#Funcion Map
def map_execution(chunk_number):
    #Sacar el nombre del hilo que se esta ejecutando
    id = current_thread().name
    
    #Inicia Proceso
    print(f"MAP Nodo {id} inicio el proceso del Chunk: {chunk_number}")
    
    #Lectura del archivo chunk
    with open(f"ficheros/chunks/{chunk_number}", "r") as reader:
        words = reader.read().split(" ")
        
        #Escritura del mapeado del archivo chunk
        for word in words:
            with open(f"ficheros/mapped_chunks/mapped_{chunk_number}", "a+") as writer:
                writer.write(f"<{word}, 1>\n")
    #Finaliza Proceso
    print(f"MAP Nodo {id} finalizo su proceso de: {chunk_number}")

    return None


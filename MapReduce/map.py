from threading import current_thread
import time

#Funcion Map
def map_execution(list_chunk_name,lock,error):
    #Sacar el nombre del hilo que se esta ejecutando
    id = current_thread().name
    
    if error==True:
            print(f"Reiniciando Hilo {id} Dañado")
            time.sleep(15)
            print(f"Hilo {id} Reparado, Iniciando Procesos")
    chunk_name=""
    while len(list_chunk_name)>0:
        
        with lock:
            chunk_name=list_chunk_name.pop(0)
        
        
        #Inicia Proceso
        print(f"MAP Nodo {id} inicio el proceso del Chunk: {chunk_name}")
        
        #Lectura del archivo chunk
        with open(f"ficheros/chunks/{chunk_name}", "r", encoding='utf-8') as reader:
            words = reader.read().split(" ")
            
            #Escritura del mapeado del archivo chunk
            for word in words:
                with open(f"ficheros/mapped_chunks/mapped_{chunk_name}", "a+", encoding='utf-8') as writer:
                    writer.write(f"<{word}, 1>\n")
        #Finaliza Proceso
        print(f"MAP Nodo {id} finalizo su proceso de: {chunk_name}")

    return None


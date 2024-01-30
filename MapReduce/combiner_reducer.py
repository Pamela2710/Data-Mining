from threading import current_thread
from collections import defaultdict
import os
import time
import json, re

def combiner(List_combiner_chunks,list_map_name,lock,error):
    """
    Combine the output of the map phase to reduce the volume of data.
    writes the combined result back to a file in the same folder with a 'combined_' prefix.
    """
    
    
    id = current_thread().name

    if error==True:
            print(f"Reiniciando Hilo {id} Dañado")
            time.sleep(15)
            print(f"Hilo {id} Reparado, Iniciando Procesos")
    chunk_name=""
    while len(list_map_name)>0:
        
        with lock:
            chunk_name=list_map_name.pop(0)
        
        List_combiner_chunks.append("combined_"+chunk_name)
        #Inicia Proceso
        dict = {}
        print(f"Combiner Nodo {id} inicio el proceso del Chunk: {chunk_name}")
        
        #Lectura del archivo chunk
        with open(f"ficheros/mapped_chunks/{chunk_name}", "r", encoding='utf-8') as reader:
            KVCount = reader.read().split("\n")

            for KV in KVCount:
    
                if KV == "":
                    continue
    
                KV_clean = re.sub(r'^<|>$', '', KV)
                KV_split = KV_clean.split(', ')
                key = KV_split[0]
                value = int(KV[KV.rfind(",") + 1: KV.rfind(">")])
    
                if key in dict.keys():
                    dict[key] = dict[key] + value
                else:
                    dict[key] = value
    
        with open(f"ficheros/map_output_folder/combined_{chunk_name}", "a+") as writer:
            for k, v in dict.items():
                writer.write(f"<{k}, {v}>\n")
            #Finaliza Proceso
            print(f"Combiner Nodo {id} finalizo su proceso de: {chunk_name}")

    return List_combiner_chunks
    
    
    
    
    
    
    """
    for filename in List_map:
        if filename.startswith("mapped_"):  # Ensure we're only processing mapped files
            path = "ficheros/map_output_folder/{filename}"
            word_counts = defaultdict(int)

            # Read the mapped file and count occurrences of each word within the chunk
            with open(path, "r") as reader:
                for line in reader:
                    word, count = line.strip()[1:-1].split(", ")
                    word_counts[word] += int(count)

            # Write the combined counts back to a new file
            combined_filename = "combined_" + filename
            with open(f"ficheros/map_output_folder/{combined_filename}", "w") as writer:
                for word, count in word_counts.items():
                    writer.write(f"<{word}, {count}>\n")

    """
    
def reducer(chunk_name,lock,error,name_part):
    """
    This function reads the combined outputs, merges them, and sums the counts for each word.
    
    :param combined_output_folder: Folder containing the combined output files from the combiner phase.
    :param final_output_path: Path to the file where the final aggregated word counts will be stored.
    """
    
    
    id = current_thread().name
    print(f"Chunk Name  {id} Dañado")
    if error==True:
            print(f"Reiniciando Hilo {chunk_name} ")
            time.sleep(15)
            print(f"Hilo {id} Reparado, Iniciando Procesos")

    #Inicia Proceso
    dict = {}

    with open(f"ficheros/sorted_chunks/{chunk_name}.txt", "r") as reader:
        KVCount = reader.read().split("\n")
        for KV in KVCount:
    
            if KV == "":
                continue
    
            KV_clean = re.sub(r'^<|>$', '', KV)
            KV_split = KV_clean.split(',[')
    
            key = KV_split[0]
    
            value = re.search(r'\[(.*?)\]', KV).group()
    
            try:
                value = json.loads(value)
                sum_value = sum(value)
            except:
                print("ERROR IN SUM", KV_clean)
    
            if key in dict.keys():
                dict[key] = dict[key] + sum_value
            else:
                dict[key] = sum_value
    
    with open(f"ficheros/{name_part}.txt", "a+") as writer:
        for k, v in dict.items():
            writer.write(f"<{k},{v}>\n")
        #Finaliza Proceso
        print(f"Reducer Nodo {id} finalizo su proceso de: {chunk_name}")

    return None
    
    
    
    
    
    
    
    
    """
    final_counts = defaultdict(int)

    # Iterate through each combined file and sum the counts for each word
    for filename in os.listdir("Data-Mining/MapReduce/ficheros/mapped_chunks"):
        if filename.startswith("combined_"):
            path = os.path.join(combined_output_folder, filename)
            with open(path, "r") as file:
                for line in file:
                    word, count = line.strip()[1:-1].split(", ")
                    final_counts[word] += int(count)

    # Write the final word counts to the specified output file
    with open("Data-Mining/MapReduce/ficheros", "w") as file:
        for word, count in final_counts.items():
            file.write(f"{word} {count}\n")
    """

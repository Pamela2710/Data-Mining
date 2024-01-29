
from collections import defaultdict
import os

def combiner(map_output_folder):
    """
    Combine the output of the map phase to reduce the volume of data.
    writes the combined result back to a file in the same folder with a 'combined_' prefix.
    """
    for filename in os.listdir(Data-Mining/MapReduce/ficheros/mapped_chunks):
        if filename.startswith("mapped_"):  # Ensure we're only processing mapped files
            path = os.path.join(map_output_folder, filename)
            word_counts = defaultdict(int)

            # Read the mapped file and count occurrences of each word within the chunk
            with open(path, "r") as file:
                for line in file:
                    word, count = line.strip()[1:-1].split(", ")
                    word_counts[word] += int(count)

            # Write the combined counts back to a new file
            combined_filename = "combined_" + filename
            with open(os.path.join(map_output_folder, combined_filename), "w") as file:
                for word, count in word_counts.items():
                    file.write(f"<{word}, {count}>\n")


def reducer(combined_output_folder, final_output_path):
    """
    This function reads the combined outputs, merges them, and sums the counts for each word.
    
    :param combined_output_folder: Folder containing the combined output files from the combiner phase.
    :param final_output_path: Path to the file where the final aggregated word counts will be stored.
    """
    final_counts = defaultdict(int)

    # Iterate through each combined file and sum the counts for each word
    for filename in os.listdir(Data-Mining/MapReduce/ficheros/mapped_chunks):
        if filename.startswith("combined_"):
            path = os.path.join(combined_output_folder, filename)
            with open(path, "r") as file:
                for line in file:
                    word, count = line.strip()[1:-1].split(", ")
                    final_counts[word] += int(count)

    # Write the final word counts to the specified output file
    with open(Data-Mining/MapReduce/ficheros, "w") as file:
        for word, count in final_counts.items():
            file.write(f"{word} {count}\n")

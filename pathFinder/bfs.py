from pathFinder import PathFinder
import json
import time


START = "Tatjana Soli"
END = "Fuglede's conjecture"

LANGUAGE = "en"
PARSED_WIKIPEDIA = "D:\\Desktop\\FIIT\\7.semester\\VI\\data\\parsed\\{lan}_parsed_sorted.jl".format(lan=LANGUAGE)
DISK_MAP = "D:\\Desktop\\FIIT\\7.semester\\VI\\data\\parsed\\{lan}_parsed_index_sorted.json".format(lan=LANGUAGE)

print("Loading disk map...")
with open(DISK_MAP, encoding="utf-8") as f:
    disk_map = json.load(f)

print("Disk map loaded")

while True:
    START = input("Starting article: ")
    END = input("Ending article: ")
    if START == "" or END == "":
        continue
    print("Searching...")
    bfs = PathFinder(START, END, disk_map, open(PARSED_WIKIPEDIA, encoding="utf-8"))
    start = time.time()
    if bfs.find_path():
        end = time.time()
        print("\n### PATH FOUND ###\n")
        bfs.print_path()
        print("\n### Time elapsed:  {time} seconds ###\n".format(time=int((end - start))))
    else:
        print("!!! PATH NOT FOUND !!!")
        print("Path does not exist")

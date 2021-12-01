import sys
import json

INPUT_FILE = sys.argv[1]


with open(INPUT_FILE) as f:
	path = json.load(f)


print()
print()
print()
print("#### PATH ####")
print()
print()


for key in path:
	# edge
	if "e" in key:
		continue

	print(path[key]["id"])
	print("---------------------------------------")
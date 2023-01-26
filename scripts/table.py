import re
import sys

with open(sys.argv[1], "r") as fin, open(sys.argv[2], "w") as fout:
	def printfile(s, e="\n"):
		print(s, file=fout, end=e)

	for i, line in enumerate(fin):
		if i == 0:
			centers = ["|c" for i in range(len(line.split(",")))] + ["|"]
			centers = "".join(centers)
			printfile(f"\\begin{{tabular}}{{{centers}}}")
			printfile("\\hline")

		line = re.sub(",", "&", line)
		line = re.sub("_", "\_", line)
		line = line[:-1] + " \\\\ \n"

		printfile(line, "")
		printfile("\\hline")

		if i == 0:
			printfile("\\hline")

	printfile("\\end{tabular}", "")

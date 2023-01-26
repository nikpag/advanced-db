#!/bin/bash

for file in ../results/**/**/part-*.csv
do
	dirname=$(dirname $file)
	mv $file $dirname/results.csv
	python3.8 table.py $dirname/results.csv $dirname/results.tex
done

#!/bin/bash

for file in ../results/**/**/*.tex
do
	dirname=$(dirname $file)
	src=$(echo $dirname/results.tex)
	dst=../tables/$(echo $dirname/results.tex | cut -d"/" -f3-5 | sed "s:/:-:g")
	cp $src $dst
done

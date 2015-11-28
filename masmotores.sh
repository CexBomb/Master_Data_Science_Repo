#! /bin/bash
FILENAME=$1
csvcut -d '^' $FILENAME -c nb_engines,manufacturer,model|sort -r -n|head -n 1|csvlook 
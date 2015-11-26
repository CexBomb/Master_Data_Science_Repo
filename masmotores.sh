#! /bin/bash
csvcut -d '^' $1 -c nb_engines,manufacturer,model|sort -r -n|head -n 1|csvlook 
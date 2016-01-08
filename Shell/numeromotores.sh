#! /bin/bash
csvcut -c nb_engines,manufacturer,model -d '^' $1 | csvgrep -c nb_engines -m $2 | csvlook
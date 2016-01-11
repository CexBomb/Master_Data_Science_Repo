#! /bin/bash
csvgrep -d '^' $1 -c manufacturer -m $2|csvcut -c nb_engines,manufacturer,model| sort -r -n|head -n 1|csvlook
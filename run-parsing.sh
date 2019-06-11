#!/bin/bash

#echo $1 $2 $3
mvn scala:run -Dlauncher=parse -DaddArgs="$1|-o|$2|-r|$3"

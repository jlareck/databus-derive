#!/bin/bash

#echo $1 $2 $3
mvn scala:run -Dlauncher=fastparse -DaddArgs="-p|$1|$2"

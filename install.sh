#!/bin/bash

set -e

build="fastparse"
if [ -f $build ]; then mv "$build" "$build.mvn"; fi

echo '#!/bin/bash' > $build

echo 'tmpfile="/tmp/$(date +%s%N)"' >> $build
echo 'touch $tmpfile' >> $build
echo 'for arg in $*; do echo $arg >> $tmpfile; done' >> $build

mvn scala:run -B -Dlauncher=fastparse -DdisplayCmd=true -DaddArgs="--help" | grep 'cmd:' | cut -c14- | sed -e 's/\/tmp\/scala-maven-.*.args/\\/g' >> $build
echo '  $tmpfile' >> $build

echo 'rm $tmpfile' >> $build

chmod +x $build

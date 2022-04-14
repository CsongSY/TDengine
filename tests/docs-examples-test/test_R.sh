#!/bin/bash

set -e

pgrep taosd || taosd >> /dev/null 2>&1 &
pgrep taosadapter || taosadapter >> /dev/null 2>&1 &

cd ../../docs-examples/R

jar_path=`find /home/debug/build -name taos-jdbcdriver-*-dist.jar`

R -f connect_native.r --args $jar_path
# R -f connect_rest.r --args $jar_path # bug 14704


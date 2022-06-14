#!/usr/bin/env bash

rm -rf build-files && \
rm -rf $HOME/anomalydetection
mvn clean package -DskipTests=true && \
echo "Completed packaging, deploying to Local" && \
mkdir -p $HOME/anomalydetection && \
cp -r build-files/* $HOME/anomalydetection && \
echo "Deployed in Local"

exit 0

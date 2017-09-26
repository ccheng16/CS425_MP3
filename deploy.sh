#!/bin/bash
javac -d ./bin/ ./src/SDS/*.java
jar -cvmf ./bin/manifest.txt ./bin/SDS.jar ./bin/SDS

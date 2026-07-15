#!/bin/bash

docker run -e executable=blob --rm -t -p 10000:10000 trekawek/azurite

#!/bin/bash

docker run -p 10000:10000 mcr.microsoft.com/azure-storage/azurite:3.7.0   --blobHost 0.0.0.0  --queueHost 0.0.0.0 --loose

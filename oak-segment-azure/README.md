See for general information: https://jackrabbit.apache.org/oak/docs/nodestore/segment/overview.html

# Unit-Tests

We are using Azurite, a Azure storage emulator. It is started automatically with the tests. https://github.com/Azure/Azurite

# Azure SDK Logging

The following steps enable the debug log of the Azure SDK: 

* add to ENV: `AZURE_LOG_LEVEL=1`
* in `oak-segment-tar` .. `logback-test.xml` set `<root level="DEBUG">`

More information: https://github.com/Azure/azure-sdk-for-java/wiki/Logging-with-Azure-SDK


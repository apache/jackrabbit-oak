<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

Connector bundle
================

`oak-search-mongot-2.5-SNAPSHOT.jar` is the prebuilt OSGi bundle for this
[source tree](..). Install this file, not the `*-tests.jar` produced by Maven.

Bundle identity:

- symbolic name: `org.apache.jackrabbit.oak-search-mongot`
- version: `2.5.0.SNAPSHOT`
- Java execution environment: Java 17 or newer
- SHA-256: `2996ff5aee0108c8b6e5a81c9dc289ef4eb2317663700266040a6e7adc48d55f`

The bundle intentionally does not embed Oak or MongoDB driver classes. The
target OSGi runtime must provide the compatible Oak 2.5 packages and MongoDB
Java Driver 5.3 bundle set: `mongodb-driver-sync`, `mongodb-driver-core`,
`bson`, and `bson-record-codec`.

To reproduce the bundle from the repository root:

    mvn -pl oak-search-mongot -am -DskipTests package

The resulting file is
`oak-search-mongot/target/oak-search-mongot-2.5-SNAPSHOT.jar`. See the module
[README](../README.md#build-and-install-the-connector-bundle) for installation
and OSGi configuration instructions.

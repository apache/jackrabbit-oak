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

# Runnable jar

Oak comes with a [runnable jar](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-run/README.md),
which contains everything you need for a simple Oak installation.


# Using Oak in your project

To use Oak in your project simply add a dependency to `org.apache.jackrabbit:oak-jcr:1.0.0` and to
`javax.jcr:jcr:2.0`:

    <dependency>
      <groupId>org.apache.jackrabbit</groupId>
      <artifactId>oak-jcr</artifactId>
      <version>1.0.0</version>
    </dependency>
    <dependency>
      <groupId>javax.jcr</groupId>
      <artifactId>jcr</artifactId>
      <version>2.0</version>
    </dependency>

Oak has simple mechanisms for constructing and configuring content repositories
for use in embedded deployments and test cases:

* [Repository construction](construct.html)
* [Configuring Oak](osgi_config.html)
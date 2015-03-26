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

# Migrating Ordered Index to Lucene Property

A quick step-by-step on how to migrate from the ordered index to
lucene.

Assuming you have the following ordered index configuration

    {
        ...
        "declaringNodeTypes" : "nt:unstructured",
        "direction" : "ascending",
        "propertyNames" : "foobar",
        "type" : "ordered"
        ...
    }

the related lucene configuration will be

    {
        "jcr:primaryType" : "oak:QueryIndexDefinition",
        "compatVersion" : 2,
        "type" : "lucene",
        "async" : "async",
        "indexRules" : {
            "jcr:primaryType" : "nt:unstructured",
            "nt:unstructured" : {
                "properties" : {
                    "jcr:primaryType" : "nt:unstructured",
                    "foobar" : {
                        "propertyIndex" : true,
                        "name" : "foobar",
                        "ordered" : true
                    }
                }
            }
        }
    }

for all the details around the configuration of Lucene index and
additional flags, please refer to the
[index documetation](lucene.html).

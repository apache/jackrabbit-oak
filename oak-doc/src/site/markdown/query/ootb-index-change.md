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

## Changing Out-Of-The-Box Index Definitions

You may have the need to change an out-of-the-box index definition
that is shipped either with oak or any other products built on top of
it.

To better deal with upgrades and changes in provided index definitions
it would be better to follow the following practice.

Let's say for example that you have the following index definition as
`NodeTypeIndex` and you'd like to add your custom node to the list:
`cust:unstructured`.

    "oak:index/nodetype" : {
      "jcr:primaryType": "oak:QueryIndexDefinition",
      "declaringNodeTypes": [
        "sling:MessageEntry",
        "slingevent:Job",
        "oak:QueryIndexDefinition",
        "rep:User",
        "rep:Authorizable",
        "sling:bgJobData",
        "sling:VanityPath",
        "sling:chunks",
        "slingevent:TimedEvent",
      ],
      "nodeTypeListDefined": true,
      "propertyNames": [
        "jcr:primaryType",
        "jcr:mixinTypes"
      ],
      "type": "property",
      "reindex": false,
      "reindexCount": 1
    }

to customise it you would do the following:

1. Copy the current index definition with a new name. Let's say
   `oak:index/custNodeType`
2. Add the custom nodetype to the `declaringNodeTypes`
3. Issue a re-index by setting `reindex=true`
4. wait for it to finish
5. either
   [disable](./query-engine.html#Temporarily_Disabling_an_Index) the
   old index definition or delete it.

The new index definition in our example, once completed would look
like the following:

    "oak:index/custNodetype" : {
      "jcr:primaryType": "oak:QueryIndexDefinition",
      "declaringNodeTypes": [
        "sling:MessageEntry",
        "slingevent:Job",
        "oak:QueryIndexDefinition",
        "rep:User",
        "rep:Authorizable",
        "sling:bgJobData",
        "sling:VanityPath",
        "sling:chunks",
        "slingevent:TimedEvent",
        "cust:unstructured"
      ],
      "nodeTypeListDefined": true,
      "propertyNames": [
        "jcr:primaryType",
        "jcr:mixinTypes"
      ],
      "type": "property",
      "reindex": false,
      "reindexCount": 2
    }

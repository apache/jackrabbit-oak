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

# Ordered Index (deprecated since 1.1.8)

Extension of the Property index will keep the order of the indexed
property persisted in the repository.

Used to speed-up queries with `ORDER BY` clause, _equality_ and
_range_ ones.

    SELECT * FROM [nt:base] ORDER BY jcr:lastModified
    
    SELECT * FROM [nt:base] WHERE jcr:lastModified > $date
    
    SELECT * FROM [nt:base] WHERE jcr:lastModified < $date
    
    SELECT * FROM [nt:base]
    WHERE jcr:lastModified > $date1 AND jcr:lastModified < $date2

    SELECT * FROM [nt:base] WHERE [jcr:uuid] = $id

To define a property index on a subtree you have to add an index
definition node that:

* must be of type `oak:QueryIndexDefinition`
* must have the `type` property set to __`ordered`__
* contains the `propertyNames` property that indicates what properties
  will be stored in the index.  `propertyNames` has to be a single
  value list of type `Name[]`

_Optionally_ you can specify

* the `reindex` flag which when set to `true`, triggers a full content
  re-index.
* The direction of the sorting by specifying a `direction` property of
  type `String` of value `ascending` or `descending`. If not provided
  `ascending` is the default.
* The index can be defined as asynchronous by providing the property
  `async=async`

_Caveats_

* In case deploying on the index on a clustered mongodb you have to
  define it as asynchronous by providing `async=async` in the index
  definition. This is to avoid cluster merges.

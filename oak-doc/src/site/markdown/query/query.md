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

## Oak Query

Oak does not index as much content by default as does Jackrabbit
2. You need to create custom indexes when necessary, much like in
traditional RDBMSs. If there is no index for a specific query, then
the repository will be traversed. That is, the query will still work
but probably be very slow.

* [The Query Engine](./query-engine.html)
* [Troubleshooting](./query-troubleshooting.html)
* [Flags](./flags.html)

### Indexes

There are 3 main types of indexes available in Oak. For other type
(eg: nodetype) please refer to the [query engine](./query-engine.html)
documentation page.

* [Lucene](./lucene.html)
* [Solr](./solr.html)
* [Property](./property-index.html)

For more details on how indexing works (for all index types):

* [Indexing](indexing.html) 
* [Reindexing](indexing.html#Reindexing)

### Customisations

* [Change Out-Of-The-Box Index Definitions](./ootb-index-change.html)
* [Machine Translation for Search](./search-mt.html)



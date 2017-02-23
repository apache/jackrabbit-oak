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
* [Flags](./flags.html)

### Indexes

There are 3 main types of indexes available in Oak. For other type
(eg: nodetype) please refer to the [query engine](./query-engine.html)
documentation page.

* [Lucene](./lucene.html)
* [Solr](./solr.html)
* [Property](./property-index.html)

#### Reindexing

Existing indexes need to be re-indexed if the definition is changed
in such a way that the index is incorrect (for example adding properties to the index).
Reindexing does not resolve other problems, such that queries not returning data.
For such cases, it is _not_ recommended to reindex 
(also because this can be very slow and use a lot of temporary disk space).
If queries don't return the right data, then possibly the index is [not yet up-to-date][OAK-5159],
or the query is incorrect, or included/excluded path settings are wrong (for Lucene indexes).
Instead of reindexing, it is suggested to first check the log file, modify the query
so it uses a different index or traversal and run the query again.
One case were reindexing can help is if the query engine picks a very slow index
for some queries because the counter index 
[got out of sync after adding and removing lots of nodes many times (fixed in recent version)][OAK-4065].
For this case, it is recommended to verify the contents of the counter index first,
and upgrade Oak before reindexing.

### Customisations

* [Change Out-Of-The-Box Index Definitions](./ootb-index-change.html)


[OAK-4065]: https://issues.apache.org/jira/browse/OAK-4065
[OAK-5159]: https://issues.apache.org/jira/browse/OAK-5159

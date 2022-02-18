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

## Elastic Index

Oak supports Elasticsearch (Elastic for short) based indexes to support 
both property constraint and full text constraints. 
Elastic indexes support similar features than [Lucene][lucene] indexes, 
however there are differences:

* The `type` is `elastic`.
* The index definition needs to be under `/oak:index`.
  Other locations are not supported.
* The `async` property needs to be set to `async`. 
  Synchronous, `nrt` or other lanes are not supported.
  Indexes are updated asynchronously.
* `refresh` is ignored.
  Changes take effect immediately after changing them.
  Existing documents in Elasticsearch are not changed.
* `reindex` is ignored.
  Indexes are automatically built when needed.
  We recommend to build then using the `oak-run` tool.
* `codec` is ignored.
* `compatVersion` is ignored.
* `useIfExists` is ignored.
* `blobSize` is ignored.
* `functionName` is ignored.
* `name` is ignored.
* `indexPath` is ignored.
* `analyzers` is ignored.
* For property definitions, `sync` and `unique` are ignored.

[lucene]: https://jackrabbit.apache.org/oak/docs/query/lucene.html

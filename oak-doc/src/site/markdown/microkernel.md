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

# MicroKernel and NodeStore

A `MicroKernel` or a `NodeStore` are ultimately implementations of the
[node state model](nodestate.html). A `MicroKernel` exposes its functionality through a String only
(JSON) API, which is easy to remote. In contrast a `NodeStore` exposes its functionality
through a pure Java API, which is easier to work with and has lower performance and memory overhead.

Oak comes with two flavours: [Segment](segmentmk.html) and [Document](documentmk.html). The former
is optimised for maximal performance in standalone deployments while the latter is optimised for
maximal scalability in clustered deployments.

## See also

* [MicroKernel integration tests](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-it/mk/README.md)
* [MicroKernel performance tests](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-mk-perf/README.md)
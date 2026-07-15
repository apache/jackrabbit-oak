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

# Node Storage

Oak comes with two node storage flavours: [Segment](segment/overview.html) and
[Document](documentmk.html). Segment storage is optimised for maximal
performance in standalone deployments, and document storage is optimised for
maximal scalability in clustered deployments.

## NodeStore API

The node storage implement the `NodeStore` APIs.
Those are ultimately representations of the
[node state model](../architecture/nodestate.html). 
The `NodeStore` exposes its functionality through a pure Java API,
which is suited to work with in Java, and has lower performance and memory overhead.

## MicroKernel API

The `MicroKernel` API was deprecated in OAK 1.2 and dropped from the project as of
Oak 1.3.0. It used to exposes its functionality through a String only (JSON) API.

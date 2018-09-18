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
# Oak Composite NodeStore

**The documentation of the Composite NodeStore implementation is work-in-progress. Please ask on oak-dev for things that are missing or unclear.**

## Overview

The `CompositeNodeStore` is a `NodeStore` implementation that wraps multiple `NodeStore` instances
and exposes them through a single API. It is possible, for instance, to store all data in a 
`DocumentNodeStore` instance and relocate `/libs` and `/apps` in a `SegmentNodeStore` instance.

Each node stored wrapped by the composite node store instance is called a _mount_.

## Design limitations

The implementation allows for a default mount, which is read-write, and for any number of 
additional mounts, which are read-only. This limitation is by design and is not expected to
be removed in future Oak version.

## Checking for read-only access

The Composite NodeStore mounts various other node stores in read-only mode. Since the read-only mode
is not enfored via permissions, it may not be queried via `Session.hasPermission`. Instead, the
read-only status is surfaced via `Session.hasCapability`. See [OAK-6563][OAK-6563] for details.

[OAK-6563]: https://issues.apache.org/jira/browse/OAK-6563

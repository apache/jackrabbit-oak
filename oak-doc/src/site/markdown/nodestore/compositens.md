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

Each node stored wrapped by the composite node store instance is called a _mount_. The
`CompositeNodeStore` can be configured with one or more mounts, each owning a defined set
of paths, and a _default mount_, owning the rest of the repository.

## Design limitations

### Read-only mounts

The implementation allows for a default mount, which is read-write, and for any number of 
additional mounts, which are read-only. This limitation is by design and is not expected to
be removed in future Oak version.

There are two major aspects to this limitation

1. Having a commit run accross two or more multiple node stores is complicated in terms of
implementation. Atomic commits will be very hard to ensure in a performant manner across
multiple stores. Additionally, it will impose implementation burders to each NodeStore
in order to support this special-case scenario.
1. There are multiple Oak subsystems that are not composite-aware and that would need to 
changed for that to happen, and this would again complicate the implementation for a
special-case scenario.

### Referenceable nodes

Referenceable nodes are not permitted in non-default mounts. The reason is cross-mount references
can become invalid in scenarios where the set of mounts changes. Consider the following scenario:

Mounts:

* default mount `D`
* non-default mount `N1`, currently mounted under /tmp
* non-default mount `N2`, currently not mounted 

In the repository, node `/content/bar` references referenceable node `/tmp/foo` (from N1). When
the repository is shut down and reconfigureed to use N2 instead of N1, the  reference can be broken
unless we ensure that the reference stores used by N1 and N2 are the same. This does not happen
today.

This constraint also means that:

* versionable nodes are not permitted in non-default mounts, as they are referenceable
* `nt:resource` nodes (usually found as children of `nt:file` nodes) are not permitted. It is recommended
  to replace them with `oak:Resource` ( see also [OAK-4567](https://issues.apache.org/jira/browse/OAK-4567) ).

## Checking for read-only access

The Composite NodeStore mounts various other node stores in read-only mode. Since the read-only mode
is not enfored via permissions, it may not be queried via `Session.hasPermission`. Instead, the
read-only status is surfaced via `Session.hasCapability`. See [OAK-6563][OAK-6563] for details.

[OAK-6563]: https://issues.apache.org/jira/browse/OAK-6563

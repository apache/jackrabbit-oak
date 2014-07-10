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

## Session refresh behavior

Oak is based on the MVCC model where each session starts with a snapshot
view of the repository. Concurrent changes from other sessions *are not
visible* to a session until it gets refreshed. A session can be refreshed
either explicitly by calling the ``refresh()`` method or implicitly by
direct-to-workspace methods or by the auto-refresh mode. Also observation
event delivery causes a session to be refreshed.

By default the auto-refresh mode automatically refreshes all sessions that
have been idle for more than one second, and it's also possible to
explicitly set the auto-refresh parameters. A typical approach would be
for long-lived admin sessions to set the auto-refresh mode to keep the
session always up to date with latest changes from the repository.

### Pattern: One session for one request/operation

One of the key patterns targeted by Oak is a web application that serves
HTTP requests. The recommended way to handle such cases is to use a
separate session for each HTTP request, and never to refresh that session.

### Anti pattern: concurrent session access

Oak is designed to be virtually lock free as long as sessions are not shared
across threads. Don't access the same session instance concurrently from
multiple threads. When doing so Oak will protect its internal data structures
from becoming corrupted but will not make any guarantees beyond that. In
particular violating clients might suffer from lock contentions or deadlocks.

If Oak detects concurrent write access to a session it will log a warning. 
For concurrent read access the warning will only be logged if `DEBUG` level 
is enabled for `org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate`.
In this case the stack trace of the other session involved will also be 
logged. For efficiency reasons the stack trace will not be logged if 
`DEBUG` level is not enabled.

### Large number of direct child node

Oak scales to large number of direct child nodes of a node as long as those
are *not* orderable. For orderable child nodes Oak keeps the order in an
internal property, which will lead to a performance degradation when the list
grows too large. For such scenarios Oak provides the ``oak:Unstructured`` node
type, which is equivalent to ``nt:unstructured`` except that it is not orderable.
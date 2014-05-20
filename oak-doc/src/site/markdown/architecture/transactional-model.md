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

Transactional model Oak sessions
================================
Oak sessions are based on a multi version concurrency control model using snapshot isolation with a
relaxed first committer wins strategy. That is, on login each session is under the impression of
operating on its own copy of the repository. Modifications from other sessions do not affect the
current session. With the relaxed first committer wins strategy a later session will fail on save
when it contains operations which are incompatible with the operations of an earlier session which
saved successfully. This is different from the standard first committer wins strategy where failure
would occur on conflicting operations rather than on incompatible operations. Incompatible is weaker
than conflict since two write operation on the same item do conflict but are not incompatible. The
details of what incompatible means are specified by `NodeStore.rebase()` and `MicroKernel.rebase()`.

Snapshot isolation exhibits [write skew](http://http//research.microsoft.com/apps/pubs/default.aspx?id=69541)
which can be problematic for some application level consistency requirements. Consider the following
sequence of operations:

    session1.getNode("/testNode").setProperty("p1", -1);
    check(session1);
    session1.save();

    session2.getNode("/testNode").setProperty("p2", -1);
    check(session2);
    session2.save();

    Session session3 = repository.login();
    check(session3);

The check method enforces an application logic constraint which says that the sum of the properties
`p1` and `p2` must not be negative. While session1 and session2 each enforce this constraint before
saving, the constraint might not hold globally for session3.

See `CompatibilityIssuesTest.sessionIsolation` for a test case demonstrating this in runnable code.

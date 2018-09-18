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

Transactional model of sessions
================================
Sessions in Oak are based on a multi version concurrency control model using snapshot isolation with
a relaxed first committer wins strategy. That is, on login each session is under the impression of
operating on its own copy of the repository. Modifications from other sessions do not affect the
current session. With the relaxed first committer wins strategy a later session will fail on save
when it contains operations which are incompatible with the operations of an earlier session which
saved successfully. This is different from the standard first committer wins strategy where failure
would occur on conflicting operations rather than on incompatible operations. Incompatible is weaker
than conflict since two write operation on the same item do conflict but are not incompatible. The
details of what incompatible is somewhat dependent on the implementation of `NodeStore.rebase()` and
the backend being used. See [below](#rebasing).

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


<a name="rebasing"/> Rebasing and incompatible changes (conflicts)
------------------------------------------------------------------
On save the changes from a session are rebased on top of the current head. That is, all changes done withing that session are re-applied on top of the latest state of the repository. This process can lead to conflicts when the latest state includes changes that are incompatible with the changes in that session. E.g. when the session modified the value of a property while in the latest state of the repository the same property changed to a different value.  

The rebasing process attempts to resolve such conflicts if possible. If a conflict is not resolvable conflicting nodes are annotated with a conflict marker denoting the type of the conflict and the value(s) before the rebase operation. The conflict marker is an internal node with the name `:conflict` and is added to the node whose properties or child nodes are in conflict.

#### Types of non resolvable conflicts
 
* `addExistingProperty`: A property has been added that has a different value than a property with the same name that has been added in trunk. 

* `removeRemovedProperty`: A property has been removed while a property of the same name has been removed in trunk. *Note:* while this conflict is technically easy to resolve, the current JCR specification mandates a conflict here. 

* `removeChangedProperty`: A property has been removed while a property of the same name has been changed in trunk. 

* `changeRemovedProperty`: A property has been changed while a property of the same name has been removed in trunk. 

* `changeChangedProperty`: A property has been changed while a property of the same name has been changed to a different value in trunk. 

* `addExistingNode`: A node has been added that is different from a node of them same name that has been added to the trunk. *Note:* Some subtleties are currently being discussed. See [OAK-1553](https://issues.apache.org/jira/browse/OAK-1553).  

* `removeRemovedNode`: A node has been removed while a node of the same name has been removed in trunk. *Note:* while this conflict is technically easy to resolve, the current JCR specification mandates a conflict here. 

* `removeChangedNode`: A node has been removed while a node of the same name has been changed in trunk. 

* `changeRemovedNode`: A node has been changed while a node of the same name has been removed in trunk. 

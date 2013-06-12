Oak MongoMK
===========

This module contains a `MicroKernel` implementation using MongoDB to persist
content.

Content Model
-------------

The `MongoMK` stores each node in a separate MongoDB document and updates to
a node are stored by adding new revision/value pairs to the document. This way
the previous state of a node is preserved and can still be retrieved by a
session looking a given snapshot (revision) of the repository.

The basic MongoDB document of a node in Oak looks like this:

    {
        "_id" : "1:/node",
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false"
        },
        "_lastRev" : {
            "1" : "r13f3875b5d1-0-1"
        },
        "_modified" : NumberLong(274208361),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "true"
        }
    }

All fields in the above document are metadata and are not exposed through the
Oak API.

The `_id` field is used as the primary key and consists of a combination of the
depth of the path and the path itself. This is an optimization to align sibling
keys in the index.

The `_deleted` sub-document contains the revision this node was created in. In
the above example the root node was created in revision `r13f3875b5d1-0-1`. If
the node is later deleted, the `_deleted` sub-document will get a new field with
the revision the node was deleted in.

The sub-document `_lastRev` contains the last revision written to this node by
each cluster node. In the above example the MongoMK cluster node with id `1`
modified the node the last time in revision `r13f3875b5d1-0-1`, when it created
the node.

The `_modified` field contains a low-resolution timestamp when the node was last
modified. The time resolution is five seconds.

Finally, the `_revision` sub-document contains commit information about changes
marked with a revision. E.g. the single entry in the above document tells us
that everything marked with revision `r13f3875b5d1-0-1` is committed and
therefore valid.

Adding a property `prop` with value `foo` to the node in a next step will
result in the following document:

    {
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false"
        },
        "_id" : "1:/node",
        "_lastRev" : {
            "1" : "r13f38818ab6-0-1"
        },
        "_modified" : NumberLong(274208516),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "true",
            "r13f38818ab6-0-1" : "true"
        },
        "prop" : {
            "r13f38818ab6-0-1" : "\"foo\""
        }
    }

Now the document contains a new sub-document with the name of the new property.
The value of the property is annotated with the revision the property was set.
With each successful commit to this node, a new field is added to the
`_revision` sub-document. Similarly the `_lastRev` sub-document and `_modified`
field are updated.

After the node is deleted the document looks like this:

    {
        "_deleted" : {
            "r13f3875b5d1-0-1" : "false",
            "r13f38835063-2-1" : "true"
        },
        "_id" : "1:/node",
        "_lastRev" : {
            "1" : "r13f38835063-2-1"
        },
        "_modified" : NumberLong(274208539),
        "_revisions" : {
            "r13f3875b5d1-0-1" : "true",
            "r13f38818ab6-0-1" : "true",
            "r13f38835063-2-1" : "true"
        },
        "prop" : {
            "r13f38818ab6-0-1" : "\"foo\""
        }
    }

The `_deleted` sub-document now contains a `r13f38835063-2-1` field marking the
node as deleted in this revision.

Reading the node in previous revisions is still possible, even if it is now
marked as deleted as of revision `r13f38835063-2-1`.

Revision Model
--------------

* Explain revision, cluster node id, etc.
* Explain branches



License
-------

(see the top-level [LICENSE.txt](../LICENSE.txt) for full license details)

Collective work: Copyright 2013 The Apache Software Foundation.

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

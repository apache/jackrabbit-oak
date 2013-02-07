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

MongoMK^2 design proposal
=========================

Segments
========

The content tree and all its revisions are stored in a collection of
immutable *segments*. Each segment is identified by a UUID and typically
contains a continuous subset of the content tree. Some segments might
also be used to store commonly occurring property values or other shared
data. Segments range from a few kilobytes to a few megabytes in size
and are stored as documents in a MongoDB collection.

Since segments are immutable, it's easy for a client to keep a local
in-memory cache of frequently accessed segments. Since segments also
leverage locality of reference, i.e. nearby nodes are often stored
in the same segment, it's common for things like small child nodes
to already exist in the cache by the time they get accessed.

Content within a segment can contain references to content within other
segments. Each segment keeps a list of the UUIDs of all other segments
it references. This list of segment references can be used to optimize
both internal storage (as seen below) and garbage collection. Segments
that are no longer referenced can be efficiently identified by
traversing the graph of segment-level references without having to
parse or even fetch the contents of each segment.

The internal record structure of nodes is described in a moment once
we first cover journal documents.

Journals
========

Journals are special, atomically updated documents that record the
state of the repository as a sequence of references to successive
root node records.

A small system could consist of just a single journal and would
serialize all repository updates through atomic updates of that journal.
A larger system that needs more write throughput can have more journals,
linked to each other in a tree hierarchy. Commits to journals in lower
levels of the tree can proceed concurrently, but will need to be
periodically merged back to the root journal. Potential conflicts and
resulting data loss or inconsistency caused by such merges can be avoided
by always committing against the root journal.

Temporary branches used for large commits are also recorded as journals.
A new private journal document is created for each branch and kept around
until the branch gets merged or discarded. Branch journals contain an
update timestamp that needs to be periodically refreshed by the client
to prevent the branch from expiring and being reclaimed by the garbage
collector.

The root node references stored in journals are used as the starting
point for garbage collection. It is assumed that all content currently
visible to clients must be accessible through at least one of the
journals. If a client wants to keep a reference to some old content
revision that's no longer referenced by one of the main journals, it
should create an empty private branch based on that revision and keep
refreshing the branch until that content is no longer needed.

Records
=======

The content inside a segment is divided in records of different types:
blocks, lists, maps, values, templates and nodes. These record types
and their internal structures are described in subsections below.

Each record is uniquely addressable by its location within the segment
and the UUID of that segment. Assuming that the size of a segment is
limited to 16MB (maximum size of a MongoDB document) and that a single
segment can contain references to up to 255 other segments, then a
reference to any record in any segment can be stored in just 4 bytes
(1 byte to identify the segment, 3 bytes for the record offset).

Block records
-------------

Blocks are binary records of up to N kB (exact size TBD, N ~ 4).
They're used as building blocks of large binary (or string) values
and stored as-is with no extra metadata or structure. Blocks are
the only record type that can't contain references to other records.

List records
------------

List records are used as components of more complex record types.
Lists are used for storing arrays of values for multivalued properties
and sequences of blocks for large binary values.

The list of references is split into pieces of up to 2^B references
each (exact size TBD, B ~ 8) and those pieces are stored as records.
If there are more than 2^B pieces like that, then a higher-level list
is created of references to those pieces. This process is continued
until the resulting list has less than 2^B entries. That top-level
list is stored as a record prefixed with the total length of the list.

The result is a hierarchically stored immutable list where each element
can be accessed in log_B(N) time and the size overhead of updating or
appending list elements (and thus creating a new immutable list) is
also log_B(N).

Map records
-----------

Like lists, maps are components of more complex record types. Maps
store unordered sets of key-value pairs of record references and are
used for nodes with a large number of properties or child nodes.

Maps are stored using the hash array mapped trie (HAMT) data structure.
The hash code of each key is split into pieces of B bits each (exact
size TBD, B ~ 6) and the keys are sorted into 2^B packs based on the
first B bits. If a pack contains less than 2^B entries, then it is
stored directly as a list of key-value pairs. Otherwise the keys are
split into subpacks based on the next B bits of their hash codes.
When all packs are stored, the list of top-level pack references gets
stored along with the total number of entries in the map.

The result is a hierarchically stored immutable map where each element
can be accessed in log_B(N) time and the size overhead of updating or
inserting list elements is also log_B(N).

Value records
-------------

Value records are byte arrays used for storing all names and values of the
content tree. Since item names can be thought of as name values and since
all JCR and Oak values can be expressed in binary form, it is easiest to
simply use that form for storing all values. The size overhead of such a
form for small value types like booleans or dates is amortized by the facts
that those types are used only for a minority of values in typical content
trees and that repeating copies of a value can be stored just once.

Small values, up to N kB (exact size TBD, N ~ 32), are stored inline in
the record, prefixed by a byte or two to indicate the length of the value.
Larger values are split into a list of fixed-size blocks and a possibly
smaller tail block, and the value is stored as a list of block references.

Template records
----------------

A template record describes the common structure of a family of related
nodes. Since the structures of most nodes in a typical content tree fall
into a small set of common templates, it makes sense to store such templates
separately instead of repeating that information separately for each node.
For example, the property names and types as well as child node names of all
nt:file nodes are typically the same. The presence of mixins and different
subtypes increases the number of different templates, but they're typically
still far fewer than nodes in the repository.

A template record consists of a set of up to N (exact size TBD, N ~ 256)
property name and type pairs. Additionally, since nodes that are empty or
contain just a single child node are most common, a template record also
contains information whether the node has zero, one or many child nodes.
In case of a single child node, the template also contains the name of
that node. For example, the template for typical mix:versionable nt:file
nodes would be (using CND-like notation):

    - jcr:primaryType (NAME)
    - jcr:mixinTypes (NAME) multiple
    - jcr:created (DATE)
    - jcr:uuid (STRING)
    - jcr:versionHistory (REFERENCE)
    - jcr:predecessors (REFERENCE) multiple
    - jcr:baseVersion (REFERENCE)
    + jcr:content

The names used in a template are stored as separate value records and
included by reference. This way multiple templates that for example all
contain the "jcr:primaryType" property name don't need to repeatedly
store it.

Node records
------------

The overall structure of the content tree is stored in node records.
Node records hold the actual content structure of the repository.

A typical node record consists of a template reference followed by
property value references (list references for multivalued properties)
and zero, one or more child node entries as indicated by the template.
If the node has more than one child nodes, then those entries are stored
as an array of name-node pairs of references.

A node that contains more than N properties or M child nodes (exact size
TBD, M ~ 1k) is stored differently, using map records for the properties
and child nodes. This way a node can become arbitrarily large and still
remain reasonably efficient to access and modify. The main downside of
this alternative storage layout is that the ordering of child nodes is
lost.
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

Segment Storage Design Overview
=========================

*NOTE:* The information on this page applies to an older version of the 
TarMK and is mainly of historical interest. For the documentation of the 
current versions see [Oak Segment Tar](segment/overview.html).

The SegmentNodeStore is an Oak storage backend that stores content as various
types of *records* within larger *segments*. One or more *journals* are
used to track the latest state of the repository. In the Tar implementation
only one "root" journal is used.

The SegmentNodeStore was designed from the ground up based on the following
key principles:

  * Immutability. Segments are immutable, which makes is easy to cache
    frequently accessed segments. This also makes it less likely for
    programming or system errors to cause repository inconsistencies, and
    simplifies features like backups or master-slave clustering.

  * Compactness. The formatting of records is optimized for size to
    reduce IO costs and to fit as much content in caches as possible.
    A node stored in SegmentNodeStore typically consumes only a fraction of the
    size it would as a bundle in Jackrabbit Classic.

  * Locality. Segments are written so that related records, like a node
    and its immediate children, usually end up stored in the same segment.
    This makes tree traversals very fast and avoids most cache misses for
    typical clients that access more than one related node per session.

This document describes the overall design of the SegmentNodeStore. See the
source code and javadocs in `org.apache.jackrabbit.oak.plugins.segment`
for full details.

Segments
========

The content tree and all its revisions are stored in a collection of
immutable segments. Each segment is identified by a UUID and typically
contains a continuous subset of the content tree, for example a node with
its properties and closest child nodes. Some segments might also be used
to store commonly occurring property values or other shared data. Segments
can be to up to 256KiB in size.

Segments come in two types: data and bulk segments. The type of a segment
is encoded in its UUID and can thus be determined already before reading
the segment. The following bit patterns are used (each `x` represents four
random bits):

  * `xxxxxxxx-xxxx-4xxx-axxx-xxxxxxxxxxxx` data segment UUID
  * `xxxxxxxx-xxxx-4xxx-bxxx-xxxxxxxxxxxx` bulk segment UUID

(This encoding makes segment UUIDs appear as syntactically valid version 4
random UUIDs specified in RFC 4122.)

Bulk segments
-------------

Bulk segments contain raw binary data, interpreted simply as a sequence
of block records with no headers or other extra metadata:

    [block 1] [block 2] ... [block N]

A bulk segment whose length is `n` bytes consists of `n div 4096` block
records of 4KiB each followed possibly a block record of `n mod 4096` bytes,
if there still are remaining bytes in the segment. The structure of a
bulk segment can thus be determined based only on the segment length.

Data segments
-------------

A data segment can contain any types of records, may refer to content in
other segments, and comes with a segment header that guides the parsing
of the segment. The overall structure of a data segment is:

    [segment header] [record 1] [record 2] ... [record N]

The header and each record is zero-padded to make their size a multiple of
four bytes and to align the next record at a four-byte boundary.

The segment header consists of the following fields:

    +--------+--------+--------+--------+--------+--------+--------+--------+
    | magic bytes: "0aK" ASCII |version |reserved|idcount |rootcount        |
    +--------+--------+--------+--------+--------+--------+--------+--------+
    | blobrefcount    | reserved (set to 0)                                 |
    +--------+--------+--------+--------+--------+--------+--------+--------+
    | Referenced segment identifiers  (idcount x 16 bytes)                  |
    |                                                                       |
    |                            ......                                     |
    |                                                                       |
    +--------+--------+--------+--------+--------+--------+--------+--------+
    | Root record references  (rootcount x 3 bytes)                         |
    |                                                                       |
    |                            ......          +--------+--------+--------+
    |                                            |                          |
    +--------+--------+--------+--------+--------+                          +
    | External blob record references  (blobrefcount x 2 bytes)             |
    |                                                                       |
    |          ......                            +--------+--------+--------+
    |                                            | padding (set to 0)       |
    +--------+--------+--------+--------+--------+--------+--------+--------+

The first three bytes of a segment always contain the ASCII string "0aK",
which is intended to make the binary segment data format easily detectable.
The next byte indicates the version of segment format, and is set to 10
for all segments that follow the format described here.

The `idcount` byte indicates how many other segments are referenced by
records within this segment. The identifiers of those segments are listed
starting at offset 16 of the segment header. This lookup table of up to
255 segment identifiers is used to optimize garbage collection and to avoid
having to repeat the 16-byte UUIDs whenever references to records in other
segments are made.

The 16-bit `rootcount` field indicates the number of root record references
that follow after the segment identifier lookup table. The root record
references are a debugging and recovery aid, that are not needed during
normal operation. They identify the types and locations of those records
within this segment that are not accessible by following references in
other records within this segment. <s>These root references give enough
context for parsing all records within a segment without any external
information.</s> See [OAK-2498](https://issues.apache.org/jira/browse/OAK-2498).

The 16-bit `blobrefcount` field indicates the number of external blob record
references that follow after the root record references. External blobs are
binary values stored in an external data store, and the blobref list makes
it possible to quickly find all such references without having to do a full
traversal of all repository content. The list of active references is given
to data store garbage collection so it won't collect the referenced binaries.

Segment Compaction
------------------

Segment compaction is needed when the repository size grows to an unmanageable
size. The growth can happen when there are a lot of repository write operations
which might create a considerable amount of content revisions that need to get
garbage collected.
The simplest solution is to schedule the revision garbage collection exposed as
a jmx operation via the `RevisionGarbageCollection` MBean. This is a non-blocking
operation, it will set an internal flag for compaction during the next `flush` call.

As a workaround in the case where the size is already too big, you might want to run
a manual compaction using the [Oak run](https://github.com/apache/jackrabbit-oak/blob/trunk/oak-run/README.md)
tool.

Journals
========

Journals are special, atomically updated documents that record the
state of the repository as a sequence of references to successive
root node records.

A small system (like Tar) could use just a single journal and would
serialize all repository updates through atomic updates of that journal.
A larger system that needs more write throughput can have more journals,
linked to each other in a tree hierarchy. Commits to journals in lower
levels of the tree can proceed concurrently, but will need to be
periodically merged back to the root journal. Potential conflicts and
resulting data loss or inconsistency caused by such merges can be avoided
by always committing against the root journal.

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
and the UUID of that segment. A single segment can contain up to 256KiB
of data and and references to up to 256 segments (including itself).
Since all records are aligned at four-byte boundaries, 16 bits are needed
to address all possible record locations within a segment. Thus only three
bytes are needed to store a reference to any record in any segment
(1 byte to identify the segment, 2 bytes for the record offset):

    +--------+--------+--------+
    | refid  | offset          |
    +--------+--------+--------+

The `refid` filed is the number of the referenced segment identifier, with
refid zero interpreted as a reference to the current segment and refids 1-255
the segment identifiers stored in the lookup table in the segment header.

Block records
-------------

Blocks are binary records of up to 4kB. They're used as building blocks
of large binary (or string) values and stored as-is with no extra metadata
or structure. Blocks are the only record type that can't contain references
to other records. Block records are typically stored in *bulk segments*
that consist only of block records and are thus easily identifiable as
containing zero references to other segments.

List records
------------

List records are used as components of more complex record types.
Lists are used for storing arrays of values for multi-valued properties
and sequences of blocks for large binary values.

The list of references is split into pieces of up to 255 references
each and those pieces are stored as records. If there are more than
255 pieces like that, then a higher-level list is created of references
to those pieces. This process is continued until the resulting list has
less than 255 entries.

    +--------+--------+--------+-----+
    | sub-list ID 1            | ... |
    +--------+--------+--------+-----+
      |
      v
    +--------+--------+--------+-----+--------+--------+--------+
    | record ID 1              | ... | record ID 255            |
    +--------+--------+--------+-----+--------+--------+--------+

The result is a hierarchically stored immutable list where each element
can be accessed in O(log N) time and the size overhead of updating or
appending list elements (and thus creating a new immutable list) is
also O(log N).

Map records
-----------

Like lists, maps are components of more complex record types. Maps
store unordered sets of key-value pairs of record references and are
used for nodes with a large number of properties or child nodes.

Maps are stored using the hash array mapped trie (HAMT) data structure.
The hash code of each key is split into pieces of 5 bits each and the
keys are sorted into 32 (2^5) buckets based on the first 5 bits. If a bucket
contains less than 32 entries, then it is stored directly as a list of
key-value pairs. Otherwise the keys are split into sub-buckets based on the
next 5 bits of their hash codes. When all buckets are stored, the list of
top-level bucket references gets stored along with the total number of
entries in the map.

The result is a hierarchically stored immutable map where each element
can be accessed in O(log N) time and the size overhead of updating or
inserting list elements is also O(log N).

TODO: Links to HAMT documentation

Value records
-------------

Value records are byte arrays used for storing all names and values of the
content tree. Since item names can be thought of as name values and since
all JCR and Oak values can be expressed in binary form (strings encoded in
UTF-8), it is easiest to simply use that form for storing all values. The
size overhead of such a form for small value types like booleans or dates
is amortized by the facts that those types are used only for a minority of
values in typical content trees and that repeating copies of a value can
be stored just once.

There are four types of value records: small, medium, long and external.
The small- and medium-sized values are stored in inline form, prepended
by one or two bytes that indicate the length of the value. Long values
of up to two exabytes (2^61) are stored as a list of block records. Finally
an external value record contains the length of the value and a string
reference (up to 4kB in length) to some external storage location.

The type of a value record is encoded in the high-order bits of the first
byte of the record. These bit patterns are:

  * `0xxxxxxx`: small value, length (0 - 127 bytes) encoded in 7 bits
  * `10xxxxxx`: medium value length (128 - 16511 bytes) encoded in 6 + 8 bits
  * `110xxxxx`: long value, length (up to 2^61 bytes) encoded in 5 + 7*8 bits
  * `1110xxxx`: external value, reference string length encoded in 4 + 8 bits

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

Tar
=====

TODO:

- tar entry checksums
- graph and index entries
- recovery mechanism
- tar generations / cleanup
- journal.log
- compaction
- cleanup
- backup
- slow startup / journal.log

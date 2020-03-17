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

# Changes in the data format

This document describes the changes in the storage format introduced by the Oak Segment Tar module.
The purpose of this document is not only to enumerate such changes, but also to explain the rationale behind them.
Pointers to Jira issues are provided for a much more terse description of changes.
Changes are presented in chronological order.

## Generation in segment headers

* Jira issue: [OAK-3348](https://issues.apache.org/jira/browse/OAK-3348)
* Since: Oak Segment Tar 0.0.2

The GC algorithm implemented by Oak Segment Tar is based on the fundamental idea of grouping records into generations.
When GC is performed, records belonging to older generations can be removed, while records belonging to newer generations have to be retained.

The fact that a record belongs to a certain generation needs to be persisted across restarts of the system. 
To not incur the size penalty of persisting the generation per record, it is persisted only once in the header of the respective segment.
Thus, the generation of a record is defined as the generation of the segment containing that record.

The original specification of the data format for the segment header left some space for future extensions.
In the new format the generation is saved at offsets 10 to 13 as a 4-byte integer value.

## Stable identifiers 

* Jira issue: [OAK-3348](https://issues.apache.org/jira/browse/OAK-3348)
* Since: Oak Segment Tar 0.0.2

The fastest way to compare two node records is to compare their addresses.
If their addresses are equal, the two node records are guaranteed to be equal.
Transitively, given that records are immutable, the subtrees identified by those node records are guaranteed to be equal.

The situation gets more complicated when the generation-based GC algorithm copies a node record over to a new generation to save it from being deleted. 
In this situation, two copies of the same node record live in two different generations, in two different segments and at two different addresses. 
To figure out whether such two node records are equal it is not sufficient to compare their addresses.

To overcome this problem, a stable identifier has been added to every node record: when a new node record is serialized, the address it is serialized to becomes its stable identifier.
The stable identifier is included in the node record and becomes part of its serialized format.
When the node record is copied to a new generation and a new segment, its address will inevitably change.
The stable identifier instead, being part of the node record itself, will not change.
This enables fast comparison between different copies of the same node records by just comparing their stable identifiers. 

The stable identifier is serialized as a 18-bytes-long string record.
This record, in turn, is referenced from the node record by adding an additional 3-bytes-long reference field to it.
In conclusion, stable identifiers add an overhead of 21 bytes to every node record in the worst case.
In the best case, the 18-bytes-long string record is shared between node records when possible, so the aforementioned overhead represents an upper limit.

## Binary references index

* Jira issue: [OAK-4201](https://issues.apache.org/jira/browse/OAK-4201)
* Since: Oak Segment Tar 0.0.4

The original data format in Oak Segment mandates that every segment maintains a list of references to external binaries.
Every time a record references an external binary - i.e. a piece of binary data that is stored in a Blob Store - a new binary reference is added to its segment.
The list of references to external binaries is inspected periodically by the Blob Store GC algorithm to determine which binaries are currently in use.
The Blob Store GC algorithm removes every binary that is not reported as used by the Segment Store.

Retrieving the comprehensive list of external binaries for the whole repository is an expensive operation wrt. I/O.
In the worst case, every segment in every TAR file has to be read from disk and the list of references to external binaries have to be parsed.
Even if a segment does not contain references to external binaries, it has to be read in memory first for the system to figure this out.

To make this process faster and and ease the pressure on I/O, Oak Segment Tar introduces an index of references to external binaries in every TAR file.
This index aggregates the required information from every segment contained in a TAR file.
When Blob Store GC is performed, instead of reading and parsing every segment, it can read and parse the index files.
This optimization reduces the amount of I/O operations significantly.

## Simplified segment and record format

* Jira issue: [OAK-4631](https://issues.apache.org/jira/browse/OAK-4631)
* Since: Oak Segment Tar 0.0.10

The former data format limited the number of references to other segments a segment could have. 
This limitation caused sub-optimal segment space utilization when a record referencing data from many different segments was written. 
In this case records quickly exhausted the hard limit on the number of references to other segments, causing a premature flush of a non-full segment.

Oak Segment Tar relaxed the limit on the number of segments to the point that it can now be considered irrelevant.
This avoids the problem of non optimal segment space utilization.
Tests show that with this change in place it is possible to store the same amount of data in a smaller amount of better utilized segments.

The Jira issue referenced in this paragraph proposes other changes other than the one discussed here.
Most of the changes proposed by the issue were subsequently reverted or never made in the code base because of their high toll on disk space.
The comments on the issue and the referenced email thread provide a more detailed insight into the various trade-offs and considerations. 

## Storage format versioning

* Jira issue: [OAK-4295](https://issues.apache.org/jira/browse/OAK-4295)
* Since: Oak Segment Tar 0.0.10

To avoid the (old) Oak Segment and the (new) Oak Segment Tar to step on each other's toes, an improved versioning mechanism of the data format was introduced.
   
First of all, the version field in the segment header has been incremented from 11 in Oak Segment to 12 in Oak Segment Tar. 
This prevents Oak Segment Tar from accessing segments written by older implementations and Oak Segment accessing segments written by newer implementations. 

This strategy has been further improved by adding a manifest file in every data folder created by Oak Segment Tar.
The manifest file is supposed to be a source of metadata for the whole repository.
Oak Segment Tar checks for the presence of a manifest file very time a data folder is open.
If a manifest file is there, the metadata has to be compatible with the current version of the currently executing code.

Repositories written by Oak Segment do not generate a manifest file while those written by Oak Segment Tar do.
This difference enables a fail-fast approach: when Oak Segment opens a data folder containing a manifest, it immediately fails complaining that the data format is too new.
When Oak Segment Tar opens a non-empty data folder without a manifest, it immediately fails complaining that the data format is too old.

## Logic record IDs

* Jira issue: [OAK-4659](https://issues.apache.org/jira/browse/OAK-4659)
* Since: Oak Segment Tar 0.0.14

In the previous implementation (Oak Segment) the position of a record in its segment is fixed.
Once written, its address consists of the identifier of its segment followed by its offset within the segment.
The offset is the effective position of the record in the segment.

This way of addressing records implies that a record can't be moved within a segment without changing its address.
Moving a record means changing its segment, its position or both and results in all reference to it being broken. 

To gain more flexibility for storing records, a new level of indirection was introduced replacing offsets with logic identifiers.
Instead of referencing a record by a segment identifier and its offset in the segment, a segment identifier and a record number is used.
The record number is a logic address for a record in the segment and is local to the segment.

With this solution the record can be moved within the segment without breaking references to it.
This change enables a number of different algorithms when it comes to garbage collection.
For example, some records can now be removed from a segment and the segment can be shrunk down by moving every remaining record next to each other.
This operation would change the position of the remaining record in the segment, but not their logic record identifier.

This change introduced a new translation table in the segment header to map record numbers to record offsets.
The table occupies 9 bytes per record (4 bytes for the record number, 1 byte for the record type and 4 bytes for the record offset).
Moreover, a new 4-bytes-long integer field has been added to the segment header containing the number of entries of the translation table.

## Root record types

* Jira issue: [OAK-2498](https://issues.apache.org/jira/browse/OAK-2498)
* Since: Oak Segment Tar 0.0.16

The record number translation table mentioned in the previous paragraph contains a 1-byte field for every record.
This field determines the type of the record referenced by that row of the table.
The change in this paragraph is about improving the information stored in the type field of the record number translation table.

The bulk of this change is the introduction of a new record type identifying records pointing to external binary data, e.g. data contained in an external Blob Store.
This is a very important information to have at hand, because it allows Oak Segment Tar to efficiently reconstruct or populate the index of binary references.
Being able to cheaply identify references to external binaries is of paramount importance when a recovery operation is performed after a crash or a data corruption.
The recovery algorithm doesn't have access to the whole repository, but needs to work on a segment at a time.
The additional record type allows the recovery algorithm to correctly recover the index of binary references even if only the segment at hand is considered without further context.
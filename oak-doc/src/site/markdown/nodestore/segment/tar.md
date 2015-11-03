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

# Structure of TAR files

Here is described the phisical layout of a TAR file as used by Apache Oak.
First, a brief introduction of the TAR format is given. Next, more details are
provided about the low level information that are written in TAR entries.
Finally, it's described how Oak saves a graph data structure inside the TAR file
and how this representation is optimized for fast retrieval.

## Organization of a TAR file

Phisically speaking, a TAR file is a linear sequence of blocks. A TAR file is
terminated by two blocks containing zero bytes. Every block has a size of 512
bytes.

Logically speaking, a TAR file is a linear sequence of entries. Every entry is
represented by two or more blocks. The first block always contains the entry
header. Subsequent blocks store the content of the file.

The entry header is composed of the following fields:

- file name (100 bytes) - name of the file stored in this entry.

- file mode (8 bytes) - string representation of the octal file mode.

- owner's numeric ID (8 bytes) - string representation of the user ID of the
  owner of the file.

- group's numeric ID (8 bytes) - string representation of the group ID of the
  owner of the file.

- file size (12 bytes) - string representation of the octal size of the file.

- last modification time (12 bytes) - string representation of the octal time
  stamp when the file was last modified.

- checksum (8 bytes) - checksum for the header data.

- file type (1 byte) - type of the file stored in the entry. This field
  specifies if the file is a regular file, a hard link or a symbolic link.

- name of linked file (1 byte) - in case the file stored in the entry is a link,
  this field stores the name of the file pointed to by the link.

## The TAR file as used by Oak

Some fields are not used by Oak. In particular, Oak sets the file mode, the
owner's numeric ID, the group's numeric ID, the checksum, and the name of linked
file to uninteresting values. The only meaningful values assigned to the fields
of the entry values are:

- file name: the name of the data file. There are different data files used by
  Oak. They are described below.

- file size: the size of the data file. The value assigned to this field is
  trivially computed from the amount of information stored in the data file.

- last modification time: the time stamp when the entry was written.

There are three kind of files stored in a TAR file:

- segments: this type of file contains data about a segment in the segment
  store. This kind of file has a file name in the form `UUID.CRC2`, where `UUID`
  is a 128 bit UUID represented as an hexadecimal string and `CRC2` is a zer-
  padded numeric string representing the CRC2 checksum of the raw segment data.

- graph: this file has a name ending in `.gph` and contains a representaion of a
  graph. The graph is represented as an adjacency list of UUIDs.

- index: this file has a name ending in `.idx` and contains a sorted list of
  every segment contained in the TAR file.

The layout of the TAR file used by Oak is engineered for perfomance of read
operations. In particular, the most important information is stored in the
bottom entries. Reading the entries from the bottom of the file, you encounter
first the index, then the graph, then segment files. The idea is that the index
must be read first, because it provides a fast tool to locate segments in the
rest of the file. Next comes the graph, that describes how segments relate to
each other. Last come the segments, whose relative order can be ignored.

At the same time, the layout of the TAR file allows fast append-only operations
when writing. Since the relative order of segment files is not important,
segment entries can be written in a first come, first served basis. The index at
the end of the file will provide a fast way to access them even if they are
scattered around the file.

## Segment files

Segment files contain raw data about a segment. Even if there are multiple kinds
of segments, TAR file only distinguishes between data and non-data segments. A
non-data segment is always saved as-is in the TAR file, without further
processing. A data segment, instead, is inspected to extract references to other
segments.

A data segment can contain at most 255 references to other segments. These
references are simply stored as a list of UUIDs. The referenced segments can be
stored inside the current TAR file or outside of it. In the first case, the
referenced segment can be found by inspecting the index. In the second case, an
external agent is responsible to find the segment in another TAR file.

The list of segments referenced by a data segment will end up in the graph file.
To speed up the process of locating a segment in the list of referenced segment,
this list is maintained ordered.

## Graph files

The graph file represents the relationships between segments stored inside or
outside the TAR file. The graph is stored as an adjacency list of UUID, where
each UUID represents a segment.

The format of the graph file is optimized for reading. The graph file is stored
in reverse order to maintain the most important information at the end of the
file. This strategy is inline with the overall layout of the entries in the TAR
file.

The content of the graph file is divided in three main parts: a graph header, a
graph adjacency list and a vertex mapping table.

The graph header contains the following fields:

- a magic number (4 bytes): identifies the beginning of a graph file.

- size of the graph adjacency list (4 bytes): number of bytes occupied by the
  graph adjacency list.

- size of the vertex mapping table (4 bytes): number of bytes occupied by the
  vertex mapping table.

- checksum (4 bytes): a CRC2 checksum of the content of the graph file.

Immediately after the graph header, the graph adjacency list is stored. In the
list, each vertex is represented by an integer. Each integer represents an index
in the vertex mapping table. For each vertex stored in the adjacency list, the
following information are written:

- the integer representing the current vertex.

- zero or more integers for each vertex referenced by the current one.

- a sentinel value representing the list of adjacent vertices (-1).

At the end, the vertex mapping table is stored. This table is just an ordered
list of UUIDs. The integers used in the graph adjacency list can be used as
index in the vertext mapping table to read the UUID of the corresponding
segment. This is a space optimization. Since UUIDs can be repeated more than
once in the adjacency list, it make sense to replace them with cheaper
placeholders. A UUID is 128 bit long, while an integer just 4.

## Index files

The index file is an ordered list of references to the entries contained in the
TAR file. The references are ordered by UUID and they point to the position in
the file where the entry is stored. Like the graph file, even the index file is
stored backwards.

The index file is divided in two parts. The first is an index header, the second
contains the real data about the index.

The index data contains the following fields:

- a magic number (4 bytes): identifies the beginning of an index file.

- size fo the index (4 bytes): number of bytes occupied by the index data. This
  size also contains padding bytes that are added to the index to make it align
  with the TAR block boundary.

- number of entries (4 bytes): how many entries the index contains.

- checksum (4 bytes): a CRC32 checksum of the content of the index file.

After the header, the content of the index starts. For every entry contained in
the index, the following information are stored:

- the most significat bits of the UUID (8 bytes)

- the least significat bits of the UUID (8 bytes)

- the offset in the TAR file where the TAR entry containing the segment is
  located.

- the size of the entry in the TAR file.

Since the entries in the index are sorted by UUID, and since the UUIDs assigned
to the entries are uniformly distributed, when searching an entry by its UUID an
efficient algorithm called interpolation search can be used. This algorithm is a
variation of binary search. While in binary search the search space (in this
case, the array of entry) is halved at every iteration, interpolation search
exploits the distribution of the keys to remove a portion of the search space
that is potentially bigger than the half of it. Interpolation search is a more
natural approximation of the way a person searches in a phone book. If the name
to search begins with the letter T, in example, it makes no sense to open the
phone book at the half. It is way more efficient, instead, to open the phone
book close to the bottom quarter, since names starting with the letter T are
more likely to be distributed in that part of the phone book.

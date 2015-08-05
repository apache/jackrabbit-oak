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

# Records and segments

While TAR files and segments are a coarse-grained mechanism to divide the
repository content in more manageable pieces, the real information is stored
inside the segments as finer-grained records. Here I zoom in the segments and
show the binary representation of data stored by Oak in the segments. It is not
strictly necessary to know how segments work in order to understand this
content, but if you feel lost you can refer to [this description of the
structure of TAR files](tar.html).

## Data and bulk segments

Segments are not created equal. Oak, in fact, distinguishes data and bulk
segments, where the former is used to store structured data (e.g. information
about node and properties), while the latter contains unstructured data (e.g.
the value of binary properties or of very long strings).

It is possible to take apart a bulk segment from a data segment by just looking
at its identifier. As explained in a previous post, a segment identifier is a
randomly generated UUID. Segment identifiers are 16 bytes long, but Oak uses 4
bits to store a flag capable to set apart bulk segments from data segments.

The most interesting kind of segment is the data segment, because it stores
information about the repository in a structured and easily accessible way.

## Overview of data segments

A data segment can be roughly divided in two parts, a header and a data section.
The header contains management information about the segment itself, while the
data section stores the actual repository data.

Repository data is split into records, that are tiny bits of information that
represent different types of information. There are different types of records,
where every type is specialized in storing a specific piece of information: node
records, template records, map records, list records, and so on.

In general, a record can be considered as a contiguous sequence of bytes stored
at a specific position inside a segment. A record can also have references to
other records, where the referenced records can be stored in the same segment or
not. Since records can reference each other, a segment actually stores a graph
of records, where the implementation guarantees that the graph is acyclic.

The segment also maintains a set of references to *root records* those records
in the graph that are not referenced by any other records. In graph jargon,
these records would be called source vertices. The set of references to root
records is stored in the header section of the segment.

## Record identifiers

Records need a mechanism to reference each other, both from inside the same
segment and across different segments. The mechanism used to reference a record
is (unsurprisingly) a record identifier.

A record identifier is composed of a *segment field* and a *position field*. The
segment field is a single byte that identifies the segment where the referenced
record is stored. The position field is the position of the record inside the
segment identified by the segment field. There are some peculiarities in both
the segment and the position field that may not be immediately obvious. The
picture below shows how a segment looks like.

![Overview of a segment](segment.png)

The segment field is just one byte long, but a segment identifier is 16 bytes
long. To bridge the gap, the segment header contains an array of segment
identifiers that is used as a look-up table. The array can store only 255
segment identifiers, so a single byte is enough to access every element in the
array. In fact, the segment field in a record identifier is just an index in the
array of segment identifiers that is used as a look-up table. The look-up table
always contains the segment identifier of the current segment in the first
position: if a segment field is set to zero, the referenced record is stored in
the current segment.

The definition of the position field relies on some important properties of data
segments:

- data segments have a maximum size of 256 KiB, or `0x40000` bytes. The size of
  a data segment can never exceed this limit, but it is perfectly legal to have
  data segments smaller than 256 KiB.

- records are always aligned on a two-bit boundaries. Stated differently, when a
  record is written in a segment, it must be stored at a position that is a
  multiple of four.

- records are stored from the end of the segment. Even if this may seem
  counterintuitive, it makes perfectly sense if you consider that new records
  are written as a consequence of an in-depth traversal of a content tree.
  Writing records from the end of the segment guarantees that the records that
  are relevant to the root of the content tree are at the beginning of the
  segment, while records that are relevant to the leaves of the content tree are
  stored at the end. This makes reading from the segment faster, because
  operating systems are optimized to read files from the beginning to the end,
  and not backwards.

So, according to the the previous properties, allowed positions range from
`0x40000` (not included) to zero (included). Moreover, assigned positions must
be multiples of four.

```
0x3FFFC, 0x3FFF8, 0x3FFF4, 0x3FFF0, 0x3FFEC, ..., 0x0
```

As you can see, three bytes would be necessary to store these positions, but we
know that a record identifier uses only two bytes to store position values. This
is possible because of a very simple optimization made to the positions before
being used in a record identifier. Since the positions are multiples of four,
the last two least significant bits are always zero. Being constant, these bits
can be removed by shifting the positions to the right twice. After the shift,
the list of possible positions become

```
0xFFFF, 0xFFFE, 0xFFFD, 0xFFFC, 0xFFFB, ..., 0x0
```

With this optimization, only two bytes are necessary to store a position inside
a record identifier. Of course, when you read a position from a record
identifier you have to remember to shift the position to left twice to obtain a
valid position.

The last important piece of information about positions is that they are always
assigned on a logic segment size of `0x40000` bytes, even if the segment ends up
to be smaller than 256 KiB. This doesn't mean that these absolute positions are
useless: in fact, they are converted to offsets relative to the effective end of
the segment.

Hopefully an example will clarify this.

Let's suppose that you are reading from a segment whose size is just 128 bytes.
Let's also suppose that you want to read a record from the position `0xFFF8`.
The problem is that the position `0xFFF8` was computed on a logical segment size
of 256 KiB (or `0x40000` bytes), so you have to convert the position `0xFFF8`
into an offset that can be used with the segment you are reading from. First of
all, the two least significant bits were stripped away from the position, so you
have to rotate `0xFFF8` two places to the left to obtain a proper position of
`0xFFF8 << 2 = 0x3FFE0`. How far was this position from the logical size of
`0x40000` bytes? It is easy to compute that the referenced record is `0x40000 -
0x3FFE0 = 0x20 = 32` bytes before the end of the segment.

Given that the size of the current segment is only 128 bytes, you can use an
absolute position of `128 - 32 = 96` in the segment to read the record you are
interested in.

## Record types

As stated before, there are many types of records. It is necessary to make a
distinction between logical and physical records, where the former are an
idealized representation of a data structure, and the latter are used to encode
the data structures in the segments as sequence of bits.

Usually there is a one-to-one mapping between logical and physical records, like
in block records, value records, template records and node records. Other types
of logical record, like map records and list records, use more than one physical
record to represent the content.

Let's give a brief description of the aforementioned records.

### Block records

A block record is the simplest form of record, because it is just a plain
sequence of bytes. It doesn't even contain a length: it is up to the writer of
this record to store the length elsewhere.

The only adjustment performed to the data is the alignment. The implementation
makes sure that the written sequence of bytes is stored at a position that is a
multiple of four.

### Value records

Value records are an improvement over block records, because they give the
possibility to store arbitrary binary data with an additional length and
optional references to other records.

The implementation represents value records in different ways, depending on the
length of the data to be written. If the data is short enough, the record can be
written in the simplest way possible: a lento field and the data inlined
directly in the record.

When the data is too big, instead, it is split into block records written into
block segments. The reference to these block records are stored into a list
record, whose identifier is stored inside the value record.

This means that value record represent a good compromise when writing binary or
string data. If the data is short enough, it is written in such a way that can
be used straight away without further reads in the segment. If the data is too
long, instead, it is stored separated from the repository content not to impact
the performance of the readers of the segment.

### List records

List records are a general-purpose list of record identifiers. They are used as
building blocks for other types of records, as we saw for value records and as
we will see for template records and node records.

The list record is a logical record using two different types of physical
records to represent itself:

- bucket record: this is a recursive record representing a list of at most 255
  references. A bucket record can reference other bucket records,
  hierarchically, or the record identifiers of the elements to be stored in the
  list. A bucket record doesn't maintain any other information exception record
  identifiers.

- list record: this is a top-level record that maintains the size of the list in
  an integer field and a record identifier pointing to a bucket.

List records are useful to store a list of references to other records. If the
list is too big, it is split into different bucket records that may be  stored
in the same segment or across segments. This guarantees good performance for
small lists, without loosing the capability to store lists with a big number of
elements.

### Map records

Map records are a general-purpose maps of strings to record identifiers. As
lists, they are used as building blocks for other types of records and are
represented using two types of physical record:

- leaf record: if the number of elements in the map is small, they are all
  stored in a leaf record. This covers the simplest case for small maps.

- branch record: if the number of elements in the map is too big, the original
  map is split into smaller maps based on a hash function applied to the keys of
  the map. A branch record is recursive, because it can reference other branch
  records if the sub-maps are too big and need to be split again.

The implementation of the map record relies on the properties defined by an
external data structure called HAMT (Hash Array Mapped Trie), capable of
combining the properties of hash table and a trie.

Map records are also optimized for small changes. In example, if only one
element of a previously stored map is modified, and the map is stored again,
only a "diff" of the map is stored. This prevents the full storage of the
modified map, which can save a considerable amount of space if the original map
was big.

### Template records

A template record stores metadata about nodes that, on average, don't change so
often. A template record stores information like the primary type, the mixin
types, the property names and the property types of a node. Having this
information stored away from the node itself prevents to write them over and
over again if they don't change when the node changes.

In example, on average, a node is created with a certain primary type and,
optionally, with some mixin types. Usually, because of its primary type, a node
is already created with a set of initial properties. After that, only the value
of the properties change, but not the structure of the node itself.

The template record allows Oak to handle simple modifications to nodes in the
most efficient way possible.

### Node records

The node record is the single most important type of record, capable of storing
both the data associated to the node and the structure of the content tree.

A node record always maintain a reference to a template record. As stated
before, a template record defines the overall structure of the node, while the
variable part of it is maintained in the node record itself.

The variable part of the node is represented by a list of property values and a
map of child nodes.

The list of property values is implemented as a list of record identifiers. For
each property in the node, its value is written in the segment. The record
identifiers referencing the values of the properties are then packed together in
a list record. The identifier of the list record is stored as part of the node
record. If the value of some properties didn't change, the previous record
identifier is just reused.

The map of child nodes is implemented as a map of record identifiers. For every
child node, its node record identifier is stored in a map indexed by name. The
map is persisted in a map record, and its identifier is stored in the node
record. Thanks to the optimizations implemented by the map record, small changes
to the map of children node don't create a lot of overhead in the segment.

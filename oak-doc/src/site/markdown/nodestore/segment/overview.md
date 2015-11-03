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

# Segment Node Store

The Segment Node Store is the implementation of the [Node
Store](../overview.html) that persists repository data on the file
system in an efficient, organized and performant way.

One of the most important tasks of the Segment Node Store is the management of a
set of TAR files. These files are the most coarse-grained containers for the
repository data. You can learn more about the general structure of a TAR file
and how Oak leverages TAR files by reading [this page](tar.html).

Every TAR file contains segments, finer-grained containers of repository data.
Unsurprisingly, segments inspired the name of this Node Store implementation.
Repository nodes and properties are serialized to one or more records, and these
records are saved into the segments. You can learn about the internal
organization of segments and the different ways to serialize records by reading
[this page](records.html).

This website also contain a broader overview of the Segment Store and of the
design desictions that brought to his implementation. The page is quite old and
potentially outdated, but contains valuable information and is accessible
[here](../segmentmk.html).

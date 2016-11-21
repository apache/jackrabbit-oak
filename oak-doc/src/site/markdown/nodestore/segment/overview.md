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

The Segment Node Store is an implementation of the Node Store that persists repository data on the file system.

The Segment Node Store serializes repository data and stores it in a set of TAR files.
These files are the most coarse-grained containers for the repository data.
You can learn more about the general structure of TAR files by reading [this page](tar.html).

Every TAR file contains segments, finer-grained containers of repository data.
Unsurprisingly, segments inspired the name of this Node Store implementation.
Repository data is serialized to one or more records, and these records are saved into the segments.
You can learn about the internal organization of segments and the different ways to serialize records by reading [this page](records.html).

This website also contains an overview of the legacy implementation of the Segment Store and of the design decisions that brought to this implementation.
The page is old and describes a deprecated implementation, but can still be accessed [here](../segmentmk.html).

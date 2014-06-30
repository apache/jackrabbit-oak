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

# Frequently asked questions

#### I get a warning "Traversed 1000 nodes ...", what does that mean?

You might be missing an index. See [Query engine](query.html).


#### I get a warning "Attempt to perform ... while another thread is concurrently ...", what is wrong?

You are accessing a `Session` instance concurrently from multiple threads. Session instances are
[not thread safe](dos_and_donts.html#Anti_pattern:_concurrent_session_access).

#### I have a SegmentMK store and the size is growing beyond control

You need to setup a regular job for [compacting the segments](nodestore/segmentmk.html#Segment_Compaction).

#### My question is not listed here

Search the [Oak dev list](http://jackrabbit.markmail.org/search/+list:org.apache.jackrabbit.oak-dev)
and the [Oak issue tracker](https://issues.apache.org/jira/browse/OAK). If you still can't find an
answer ask on [the list](participating.html).



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

Jackrabbit Oak - the next generation content repository
=======================================================

Jackrabbit Oak is an effort to implement a scalable and performant hierarchical content repository
for use as the foundation of modern world-class web sites and other demanding content applications.
The Oak effort is a part of the [Apache Jackrabbit project](http://jackrabbit.apache.org/). Apache
Jackrabbit is a project of the [Apache Software Foundation](http://www.apache.org/).

Why Oak
-------

Jackrabbit 2.x is a solid and feature-rich content repository that works well especially for the
needs of traditional web sites and integrated content management applications. However, the trends
in user expectations (especially for personalized, interactive and collaborative content),
application architectures (distributed, loosely coupled, multi-platform solutions with lots of data)
and hardware design (horizontal rather than vertical scaling) have rendered some of the original
Jackrabbit design decisions (which date back almost a decade) obsolete and there is no easy way to
incrementally update the design.

Jackrabbit Oak aims to implement a scalable and performant hierarchical content repository for use
as the foundation of modern world-class web sites and other demanding content applications. The
repository should implement standards like JCR, WebDAV and CMIS, and be easily accessible from
various platforms, especially from JavaScript clients running in modern browser environments. The
implementation should provide more out-of-the-box functionality than typical NoSQL databases while
achieving comparable levels of scalability and performance.

  
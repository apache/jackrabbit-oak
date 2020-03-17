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

Oak API
--------------------------------------------------------------------------------


- [Javadocs](../apidocs/) (latest release)
- Javadoc of previous releases are available from [javadoc.io](http://www.javadoc.io/): 
    - [oak-jcr](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-jcr/)
    - [oak-core](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-core/)
    - [oak-run](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-run/)
    - [oak-upgrade](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-upgrade/)
    - [oak-commons](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-commons/)
    - [oak-blob](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-blob/)
    - [oak-blob-cloud](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-blob-cloud/)
    - [oak-http](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-http/)
    - [oak-lucene](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-lucene/)
    - [oak-solr-core](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-solr-core/)
    - [oak-solr-osgi](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-solr-osgi/)
    - [oak-auth-external](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-auth-external/)
    - [oak-auth-ldap](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-auth-ldap/)
    - [oak-segment-tar](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-segment-tar/)
    - [oak-authorization-cug](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-authorization-cug/)
    - [oak-exercise](http://www.javadoc.io/doc/org.apache.jackrabbit/oak-exercise/)

### Key API entry points
- [ContentRepository]
- [ContentSession]
- [Root]
- [Tree]
- [NodeState]
- [PropertyState]

#### Values
- [PropertyValue]
- [Type]
- [Blob]

#### Query
- [QueryEngine]
- [Query]
- [ResultRow]

#### Various
- [AuthInfo] : see section [Authentication](../security/authentication.html)
- [Descriptors]
- [CommitFailedException] : see also [Error Codes](error_codes.html)


<!-- hidden references -->
[ContentRepository]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/ContentRepository.html
[ContentSession]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/ContentSession.html
[Root]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/Root.html
[Tree]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/Tree.html
[PropertyState]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/PropertyState.html
[NodeState]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/NodeState.html
[PropertyValue]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/PropertyValue.html
[Type]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/Type.html
[Blob]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/Blob.html
[QueryEngine]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/QueryEngine.html
[Query]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/Query.html
[ResultRow]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/ResultRow.html
[AuthInfo]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/AuthInfo.html
[Descriptors]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/Descriptors.html
[CommitFailedException]: /oak/docs/apidocs/org/apache/jackrabbit/oak/api/CommitFailedException.html

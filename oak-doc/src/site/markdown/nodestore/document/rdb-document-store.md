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
# RDB DocumentStore

The `RDBDocumentStore` is one of the backend implementations for the
[DocumentNodeStore](../documentmk.html). It uses relational databases to
persist nodes as documents, mainly emulating the native capabilities of
[MongoDocumentStore](mongo-document-store.html).

Note that the [API docs for RDBDocumentStore](/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/document/rdb/RDBDocumentStore.html)
contain more implementation details.

## Supported Databases

The code was written to be as database-agnostic as possible. That said, there 
are vendor-specific code paths. Adding support for a new database type
however should be relatively straighforward. Most of the database-specific
code resides in the `RDBDocumentStoreDB` class.

The following databases are supported in the sense that they are recognized
and have been tested with:

For testing purposes:

- Apache Derby
- H2DB

For production use:

- IBM DB2 (LUW)
- Microsoft SQL Server
- MySQL (MariaDB)
- Oracle
- PostgreSQL

For supported databases, `RDBDocumentStoreDB` has knowledge about supported
versions (and likewise supported JDBC drivers). Watch out for log messages
during system startup which might warn about outdated versions (the system
will attempt to start anyway).


## Database Creation

`RDBDocumentStore` relies on JDBC, and thus, in general, can not create
database instances (that said, certain DBs such as Apache Derby or H2DB can create the
database automatically when it's not there yet - consult the DB documentation
in general and the JDBC URL syntax specifically).

So in general, the administrator will have to take care of creating the database.
There are only a few requirements for the database, but these are critical for
the correct operation:

- character fields must be able to store any Unicode code point - UTF-8 encoding is recommended
- the collation for character fields needs to sort by Unicode code points

## Table Creation

The implementation will try to create all tables and indices when they are not present
yet. Of course this requires that the configured database user actually has
permission to do so. If it does not, the system will not start up and provide
diagnostics in the log file.

Administrators who want to create tables upfront can do so. The DDL statements
for the supported databases can be dumped using [RDBHelper](/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/document/rdb/RDBHelper.html).


## Upgrade from earlier versions

As of Oak 1.8, the database layout has been slightly extended (see 
[API docs for RDBDocumentStore](/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/document/rdb/RDBDocumentStore.html)
for details).

Upon startup on an "old" database instance, `RDBDocumentStore` will try to
upgrade the tables. If this fails, it will continue using the "old" layout,
and log diagnostics about the failed upgrade. The upgrade can then be done
at a later point of time by executing the required DDL statements.





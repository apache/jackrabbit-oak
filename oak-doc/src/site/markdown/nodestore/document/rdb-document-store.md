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
will attempt to start anyway):

~~~
12:20:20.864 ERROR [main] RDBDocumentStore.java:1014        Unsupported Apache Derby version: 9.14, expected at least 10.11
~~~


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

## <a name="table-creation"></a> Table Creation

The implementation will try to create all tables and indices when they are not present
yet. Of course this requires that the configured database user actually has
permission to do so. Example from system log:

~~~
12:20:22.705 INFO  [main] RDBDocumentStore.java:1063        RDBDocumentStore (SNAPSHOT) instantiated for database Apache Derby 10.14.2.0 - (1828579) (10.14), using driver: Apache Derby Embedded JDBC Driver 10.14.2.0 - (1828579) (10.14), connecting to: jdbc:derby:./target/derby-ds-test, transaction isolation level: TRANSACTION_READ_COMMITTED (2), SA.NODES: ID VARCHAR(512), MODIFIED BIGINT, HASBINARY SMALLINT, DELETEDONCE SMALLINT, MODCOUNT BIGINT, CMODCOUNT BIGINT, DSIZE BIGINT, VERSION SMALLINT, SDTYPE SMALLINT, SDMAXREVTIME BIGINT, DATA VARCHAR(16384), BDATA BLOB(1073741824) /* {BIGINT=-5, BLOB=2004, SMALLINT=5, VARCHAR=12} */ /* index NODES_MOD on SA.NODES (MODIFIED ASC) other (#0, p0), index NODES_SDM on SA.NODES (SDMAXREVTIME ASC) other (#0, p0), index NODES_SDT on SA.NODES (SDTYPE ASC) other (#0, p0), index NODES_VSN on SA.NODES (VERSION ASC) other (#0, p0), unique index SQL190131122022490 on SA.NODES (ID ASC) other (#0, p0) */
12:20:22.705 INFO  [main] RDBDocumentStore.java:1070        Tables created upon startup: [CLUSTERNODES, NODES, SETTINGS, JOURNAL]
~~~

If it does not, the system will not start up and provide
diagnostics in the log file.

Administrators who want to create tables upfront can do so. The DDL statements
for the supported databases can be dumped using [RDBHelper](/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/document/rdb/RDBHelper.html)
or, more recently, using `oak-run rdbddldump` (see [below](#rdbddldump)).


## <a name="upgrade"></a> Upgrade from earlier versions

As of Oak 1.8, the database layout has been slightly extended (see 
[API docs for RDBDocumentStore](/oak/docs/apidocs/org/apache/jackrabbit/oak/plugins/document/rdb/RDBDocumentStore.html#apidocs.versioning)
for details).

Upon startup on an "old" database instance, `RDBDocumentStore` will try to
upgrade the tables. Example (for `NODES`):

~~~
12:05:54.146 INFO  [main] RDBDocumentStore.java:1369        Upgraded NODES to DB level 1 using 'alter table NODES add VERSION smallint'

12:05:54.166 INFO  [main] RDBDocumentStore.java:1369        Upgraded NODES to DB level 2 using 'alter table NODES add SDMAXREVTIME bigint'
12:05:54.167 INFO  [main] RDBDocumentStore.java:1369        Upgraded NODES to DB level 2 using 'create index NODES_VSN on NODES (VERSION)'
12:05:54.167 INFO  [main] RDBDocumentStore.java:1369        Upgraded NODES to DB level 2 using 'create index NODES_SDT on NODES (SDTYPE)'
12:05:54.167 INFO  [main] RDBDocumentStore.java:1369        Upgraded NODES to DB level 2 using 'create index NODES_SDM on NODES (SDMAXREVTIME)'
~~~

If this fails, it will continue using the "old" layout,
and log diagnostics about the failed upgrade:

~~~
12:05:56.746 INFO  [main] RDBDocumentStore.java:1379        Attempted to upgrade NODES to DB level 1 using 'alter table NODES add VERSION smallint', but failed with SQLException 'table alter statement rejected: alter table NODES add VERSION smallint' (code: 17/state: ABCDE) - will continue without.

12:05:56.955 INFO  [main] RDBDocumentStore.java:1379        Attempted to upgrade NODES to DB level 2 using 'alter table NODES add SDTYPE smallint', but failed with SQLException 'table alter statement rejected: alter table NODES add SDTYPE smallint' (code: 17/state: ABCDE) - will continue without.
12:05:56.955 INFO  [main] RDBDocumentStore.java:1379        Attempted to upgrade NODES to DB level 2 using 'alter table NODES add SDMAXREVTIME bigint', but failed with SQLException 'table alter statement rejected: alter table NODES add SDMAXREVTIME bigint' (code: 17/state: ABCDE) - will continue without.
12:05:56.964 INFO  [main] RDBDocumentStore.java:1379        Attempted to upgrade NODES to DB level 2 using 'create index NODES_SDT on NODES (SDTYPE)', but failed with SQLException ''SDTYPE' is not a column in table or VTI 'NODES'.' (code: 20000/state: 42X14) - will continue without.
12:05:56.964 INFO  [main] RDBDocumentStore.java:1379        Attempted to upgrade NODES to DB level 2 using 'create index NODES_SDM on NODES (SDMAXREVTIME)', but failed with SQLException ''SDMAXREVTIME' is not a column in table or VTI 'NODES'.' (code: 20000/state: 42X14) - will continue without.
~~~

The upgrade can then be done
at a later point of time by executing the required DDL statements.

## <a name="rdbddldump"></a> oak-run rdbddldump

`@since Oak 1.8.12` `@since Oak 1.10.1` `@since Oak 1.12`

The `rdbddldump` prints out the DDL statements that Oak would use to create or
update a database. It can be used to create the tables upfront, or to obtain
the DDL statements needed to upgrade to a newer schema version.

By default, it will print out the DDL statements for all supported databases,
with a target of the latest schema version.

The `--db` switch can be used to specify the database type (note that precise
spelling is needed, otherwise the code will fall back to a generic database 
type).

The `--initial` switch selects the initial database schema (and defaults to
the most recent one).

The `--upgrade` switch selects the target database schema (and defaults to
the most recent one).

Selecting a higher "upgrade" version then the "initial" version causes the
tool to create separate DDL statements for the initial table schema (which may
already be there), and then to add individual statements for the upgrade to
the target schema.

For instance:

~~~
java -jar oak-run-*.jar rdbddldump --db DB2 --initial 0 --upgrade 2
~~~

will dump statements for DB2, initially creating schema version 0 tables,
and then include DDL statements to upgrade to version 2 (the latter would
be applicable if an installation needed to be upgraded from an Oak version
older than 1.8 to 1.8 or newer).

~~~
-- DB2

  -- creating table CLUSTERNODES for schema version 0
  create table CLUSTERNODES (ID varchar(512) not null, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA blob(1073741824))
  create unique index CLUSTERNODES_pk on CLUSTERNODES ( ID ) cluster
  alter table CLUSTERNODES add constraint CLUSTERNODES_pk primary key ( ID )
  create index CLUSTERNODES_MOD on CLUSTERNODES (MODIFIED)
  -- upgrading table CLUSTERNODES to schema version 1
  alter table CLUSTERNODES add VERSION smallint
  -- upgrading table CLUSTERNODES to schema version 2
  alter table CLUSTERNODES add SDTYPE smallint
  alter table CLUSTERNODES add SDMAXREVTIME bigint
  create index CLUSTERNODES_VSN on CLUSTERNODES (VERSION)
  create index CLUSTERNODES_SDT on CLUSTERNODES (SDTYPE) exclude null keys
  create index CLUSTERNODES_SDM on CLUSTERNODES (SDMAXREVTIME) exclude null keys

  -- creating table JOURNAL for schema version 0
  create table JOURNAL (ID varchar(512) not null, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA blob(1073741824))
  create unique index JOURNAL_pk on JOURNAL ( ID ) cluster
  alter table JOURNAL add constraint JOURNAL_pk primary key ( ID )
  create index JOURNAL_MOD on JOURNAL (MODIFIED)
  -- upgrading table JOURNAL to schema version 1
  alter table JOURNAL add VERSION smallint
  -- upgrading table JOURNAL to schema version 2
  alter table JOURNAL add SDTYPE smallint
  alter table JOURNAL add SDMAXREVTIME bigint
  create index JOURNAL_VSN on JOURNAL (VERSION)
  create index JOURNAL_SDT on JOURNAL (SDTYPE) exclude null keys
  create index JOURNAL_SDM on JOURNAL (SDMAXREVTIME) exclude null keys

  -- creating table NODES for schema version 0
  create table NODES (ID varchar(512) not null, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA blob(1073741824))
  create unique index NODES_pk on NODES ( ID ) cluster
  alter table NODES add constraint NODES_pk primary key ( ID )
  create index NODES_MOD on NODES (MODIFIED)
  -- upgrading table NODES to schema version 1
  alter table NODES add VERSION smallint
  -- upgrading table NODES to schema version 2
  alter table NODES add SDTYPE smallint
  alter table NODES add SDMAXREVTIME bigint
  create index NODES_VSN on NODES (VERSION)
  create index NODES_SDT on NODES (SDTYPE) exclude null keys
  create index NODES_SDM on NODES (SDMAXREVTIME) exclude null keys

  -- creating table SETTINGS for schema version 0
  create table SETTINGS (ID varchar(512) not null, MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, DATA varchar(16384), BDATA blob(1073741824))
  create unique index SETTINGS_pk on SETTINGS ( ID ) cluster
  alter table SETTINGS add constraint SETTINGS_pk primary key ( ID )
  create index SETTINGS_MOD on SETTINGS (MODIFIED)
  -- upgrading table SETTINGS to schema version 1
  alter table SETTINGS add VERSION smallint
  -- upgrading table SETTINGS to schema version 2
  alter table SETTINGS add SDTYPE smallint
  alter table SETTINGS add SDMAXREVTIME bigint
  create index SETTINGS_VSN on SETTINGS (VERSION)
  create index SETTINGS_SDT on SETTINGS (SDTYPE) exclude null keys
  create index SETTINGS_SDM on SETTINGS (SDMAXREVTIME) exclude null keys

   -- creating blob store tables
  create table DATASTORE_META (ID varchar(64) not null primary key, LVL int, LASTMOD bigint)
  create table DATASTORE_DATA (ID varchar(64) not null primary key, DATA blob(2097152))
~~~


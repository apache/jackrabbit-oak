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


## <a name="database-creation"></a> Database Creation

`RDBDocumentStore` relies on JDBC, and thus, in general, can not create
database instances (that said, certain DBs such as Apache Derby or H2DB can create the
database automatically when it's not there yet - consult the DB documentation
in general and the JDBC URL syntax specifically).

So in general, the administrator will have to take care of creating the database.
There are only a few requirements for the database, but these are critical for
the correct operation:

- character fields must be able to store any Unicode code point - UTF-8 encoding is recommended
- the collation for character fields needs to sort by Unicode code points
- BLOBs need to support sizes of ~16MB

The subsections below give examples that have been found to work during the
development of `RDBDocumentStore`.


### <a name="database-creation-db2"></a> DB2

Creating a database called `OAK`:
~~~
create database oak USING CODESET UTF-8 TERRITORY DEFAULT COLLATE USING IDENTITY;
~~~

To verify, check the INFO level log message written by `RDBDocumentStore`
upon startup. For example:

~~~
14:47:20.332 INFO  [main] RDBDocumentStore.java:1065        RDBDocumentStore (SNAPSHOT) instantiated for database DB2/NT64 SQL11014 (11.1), using driver: IBM Data Server Driver for JDBC and SQLJ 4.19.77 (4.19), connecting to: jdbc:db2://localhost:50276/OAK, properties: {DB2ADMIN.CODEPAGE=1208, DB2ADMIN.COLLATIONSCHEMA=SYSIBM, DB2ADMIN.COLLATIONNAME=IDENTITY}, transaction isolation level: TRANSACTION_READ_COMMITTED (2), DB2ADMIN.NODES: ID VARCHAR(512), MODIFIED BIGINT, HASBINARY SMALLINT, DELETEDONCE SMALLINT, MODCOUNT BIGINT, CMODCOUNT BIGINT, DSIZE BIGINT, VERSION SMALLINT, SDTYPE SMALLINT, SDMAXREVTIME BIGINT, DATA VARCHAR(16384), BDATA BLOB(1073741824) /* {BIGINT=-5, BLOB=2004, SMALLINT=5, VARCHAR=12} */ /* index DB2ADMIN.NODES_MOD on DB2ADMIN.NODES (MODIFIED ASC) other (#0, p0), unique index DB2ADMIN.NODES_PK on DB2ADMIN.NODES (ID ASC) clustered (#0, p0), index DB2ADMIN.NODES_SDM on DB2ADMIN.NODES (SDMAXREVTIME ASC) other (#0, p0), index DB2ADMIN.NODES_SDT on DB2ADMIN.NODES (SDTYPE ASC) other (#0, p0), index DB2ADMIN.NODES_VSN on DB2ADMIN.NODES (VERSION ASC) other (#0, p0) */
~~~


### <a name="database-creation-mysql"></a> MySQL

Creating a database called `OAK`:
~~~
create database oak DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
~~~

Also make sure to configure the `max_allowed_packet` parameter
for the server (mysqld) to a value greater than 4M (such as 8388608).

To verify, check the INFO level log message written by `RDBDocumentStore`
upon startup. For example:

~~~
13:40:46.637 INFO  [main] RDBDocumentStore.java:1065        RDBDocumentStore (SNAPSHOT) instantiated for database MySQL 8.0.15 (8.0), using driver: MySQL Connector/J mysql-connector-java-8.0.15 (Revision: 79a4336f140499bd22dd07f02b708e163844e3d5) (8.0), connecting to: jdbc:mysql://localhost:3306/oak?serverTimezone=UTC, properties: {character_set_database=utf8mb4, character_set_client=utf8mb4, character_set_connection=utf8mb4, character_set_results=, max_allowed_packet=8388608, collation_database=utf8mb4_unicode_ci, character_set_system=utf8, collation_server=utf8mb4_0900_ai_ci, collation=utf8mb4_unicode_ci, character_set_filesystem=binary, character_set_server=utf8mb4, collation_connection=utf8mb4_0900_ai_ci}, transaction isolation level: TRANSACTION_REPEATABLE_READ (4), .nodes: ID VARBINARY(512), MODIFIED BIGINT(20), HASBINARY SMALLINT(6), DELETEDONCE SMALLINT(6), MODCOUNT BIGINT(20), CMODCOUNT BIGINT(20), DSIZE BIGINT(20), VERSION SMALLINT(6), SDTYPE SMALLINT(6), SDMAXREVTIME BIGINT(20), DATA VARCHAR(16000), BDATA LONGBLOB(2147483647) /* {BIGINT=-5, LONGBLOB=-4, SMALLINT=5, VARBINARY=-3, VARCHAR=12} */ /* unique index oak.PRIMARY on nodes (ID ASC) other (#0, p0), index oak.NODES_MOD on nodes (MODIFIED ASC) other (#0, p0), index oak.NODES_SDM on nodes (SDMAXREVTIME ASC) other (#0, p0), index oak.NODES_SDT on nodes (SDTYPE ASC) other (#0, p0), index oak.NODES_VSN on nodes (VERSION ASC) other (#0, p0) */
~~~
 

### <a name="database-creation-oracle"></a> Oracle

Creating a database called `OAK`:

(to be done)

To verify, check the INFO level log message written by `RDBDocumentStore`
upon startup. For example:

~~~
13:26:37.073 INFO  [main] RDBDocumentStore.java:1067        RDBDocumentStore (SNAPSHOT) instantiated for database Oracle Oracle Database 12c Enterprise Edition Release 12.2.0.1.0 - 64bit Production (12.2), using driver: Oracle JDBC driver 12.2.0.1.0 (12.2), connecting to: jdbc:oracle:thin:@localhost:1521:orcl, properties: {NLS_CHARACTERSET=AL32UTF8, NLS_COMP=BINARY}, transaction isolation level: TRANSACTION_READ_COMMITTED (2), .: ID VARCHAR2(512), MODIFIED NUMBER, HASBINARY NUMBER, DELETEDONCE NUMBER, MODCOUNT NUMBER, CMODCOUNT NUMBER, DSIZE NUMBER, VERSION NUMBER, SDTYPE NUMBER, SDMAXREVTIME NUMBER, DATA VARCHAR2(4000), BDATA BLOB(-1) /* {BLOB=2004, NUMBER=2, VARCHAR2=12} */ /* index NODES_MOD on SYSTEM.NODES (MODIFIED) clustered (#0, p0), index NODES_SDM on SYSTEM.NODES (SDMAXREVTIME) clustered (#0, p0), index NODES_SDT on SYSTEM.NODES (SDTYPE) clustered (#0, p0), index NODES_VSN on SYSTEM.NODES (VERSION) clustered (#0, p0), unique index SYS_C008093 on SYSTEM.NODES (ID) clustered (#0, p0) */
~~~


### <a name="database-creation-postgresql"></a> PostgreSQL

Creating a database called `OAK`:
~~~
CREATE DATABASE "oak" TEMPLATE = template0 ENCODING = 'UTF8' LC_COLLATE = 'C' LC_CTYPE = 'C';
~~~

To verify, check the INFO level log message written by `RDBDocumentStore`
upon startup. For example:

~~~
16:26:28.172 INFO  [main] RDBDocumentStore.java:1065        RDBDocumentStore (SNAPSHOT) instantiated for database PostgreSQL 10.6 (10.6), using driver: PostgreSQL JDBC Driver 42.2.5 (42.2), connecting to: jdbc:postgresql:oak, properties: {datcollate=C, pg_encoding_to_char(encoding)=UTF8}, transaction isolation level: TRANSACTION_READ_COMMITTED (2), .nodes: id varchar(512), modified int8, hasbinary int2, deletedonce int2, modcount int8, cmodcount int8, dsize int8, version int2, sdtype int2, sdmaxrevtime int8, data varchar(16384), bdata bytea(2147483647) /* {bytea=-2, int2=5, int8=-5, varchar=12} */ /* index nodes_mod on public.nodes (modified ASC) other (#0, p1), unique index nodes_pkey on public.nodes (id ASC) other (#0, p1), index nodes_sdm on public.nodes (sdmaxrevtime ASC) other (#0, p1), index nodes_sdt on public.nodes (sdtype ASC) other (#0, p1), index nodes_vsn on public.nodes (version ASC) other (#0, p1) */
~~~
 
 
### <a name="database-creation-sqlserver"></a> SQL Server

Creating a database called `OAK`:
~~~
create database OAK;
~~~

To verify, check the INFO level log message written by `RDBDocumentStore`
upon startup. For example:

~~~
16:59:12.726 INFO  [main] RDBDocumentStore.java:1067        RDBDocumentStore (SNAPSHOT) instantiated for database Microsoft SQL Server 13.00.5081 (13.0), using driver: Microsoft JDBC Driver 7.2 for SQL Server 7.2.1.0 (7.2), connecting to: jdbc:sqlserver://localhost:1433;useBulkCopyForBatchInsert=false;cancelQueryTimeout=-1;sslProtocol=TLS;jaasConfigurationName=SQLJDBCDriver;statementPoolingCacheSize=0;serverPreparedStatementDiscardThreshold=10;enablePrepareOnFirstPreparedStatementCall=false;fips=false;socketTimeout=0;authentication=NotSpecified;authenticationScheme=nativeAuthentication;xopenStates=false;sendTimeAsDatetime=true;trustStoreType=JKS;trustServerCertificate=false;TransparentNetworkIPResolution=true;serverNameAsACE=false;sendStringParametersAsUnicode=true;selectMethod=direct;responseBuffering=adaptive;queryTimeout=-1;packetSize=8000;multiSubnetFailover=false;loginTimeout=15;lockTimeout=-1;lastUpdateCount=true;encrypt=false;disableStatementPooling=true;databaseName=OAK;columnEncryptionSetting=Disabled;applicationName=Microsoft JDBC Driver for SQL Server;applicationIntent=readwrite;, properties: {collation_name=Latin1_General_CI_AS}, transaction isolation level: TRANSACTION_READ_COMMITTED (2), .: ID varbinary(512), MODIFIED bigint, HASBINARY smallint, DELETEDONCE smallint, MODCOUNT bigint, CMODCOUNT bigint, DSIZE bigint, VERSION smallint, SDTYPE smallint, SDMAXREVTIME bigint, DATA nvarchar(4000), BDATA varbinary(2147483647) /* {bigint=-5, nvarchar=-9, smallint=5, varbinary=-3} */ /* index NODES.NODES_MOD on dbo.NODES (MODIFIED ASC) other (#0, p0), unique index NODES.NODES_PK on dbo.NODES (ID ASC) clustered (#0, p0), index NODES.NODES_SDM on dbo.NODES (SDMAXREVTIME ASC) other (#0, p0), index NODES.NODES_SDT on dbo.NODES (SDTYPE ASC) other (#0, p0), index NODES.NODES_VSN on dbo.NODES (VERSION ASC) other (#0, p0) */
~~~


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


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

## Index Definition Audit

See [OAK-12458](https://issues.apache.org/jira/browse/OAK-12458).

Changes of index definitions can be recorded in the repository,
in the same commit as the change itself.
This makes it possible to find out when, by whom and how an index definition was changed,
also when old revisions of the repository are no longer available.
The recorded changes can be read, and index definitions can be disabled, enabled and deleted,
with the "Index Manager" MBean.

### Enabling the Audit

The audit is disabled by default. It is enabled by either:

* the feature toggle `FT_OAK-12458`, or
* the system property `oak.indexDefinitionAudit=true`.
  This is needed for tools that change index definitions outside of the repository process
  (for example oak-run, when importing indexes), where feature toggles are not available.

Index imports started within the repository process (`IndexerMBean.importIndex`) are recorded
if the feature toggle is enabled.
If recording a change fails, a warning is logged, and the change itself is committed.

Changes made while the audit is disabled are not recorded.
The first entry of an index that existed before the audit was enabled
is therefore the first change made after that.
The feature toggle is a setting of each repository instance:
in a cluster, it needs to be enabled on every cluster node,
and the system property needs to be set for every tool that changes index definitions,
otherwise their changes are missing from the audit trail.

### What is Recorded

Changes are recorded by the synchronous index update,
for each index definition below an `oak:index` node.
The changes are compared before Oak itself changes the index definitions in the same commit
(for example resetting the `reindex` flag), so that only the changes of the commit are recorded.

| Operation | Recorded when the commit... |
|-----------|-----------------------------|
| `CREATED` | adds an index definition |
| `MERGED` | adds an index definition that has a `merges` property (a merged index, see [Diff Indexes](indexing.html#diff-indexes)) |
| `CHANGED` | changes an index definition |
| `DISABLED` | sets the type to `disabled`, or adds an index definition that is already disabled |
| `ENABLED` | changes the type from `disabled` to another type |
| `REINDEX` | only sets `reindex` to `true`, or imports an existing index with the index importer (out-of-band indexing) |
| `DELETED` | removes an index definition |

Each entry contains:

* `operation`, `time`
* `user`: the user of the commit; for a disable or delete, the value of the `disabledBy`
  (set in the same commit) or `deletedBy` (set before the deletion) property if a tool sets it
* `reason`: the reason given to the Index Manager, or the value of `disabledReason` set in the same commit
* `origin`: the session id of the commit; `IndexImporter` for imported indexes,
  `PurgeOldIndexVersion` for the purge of old index versions with oak-run,
  and `IndexManager` for changes made with the MBean
* `thread`: the name of the thread; for changes made with the MBean, `IndexManager.<operation>`
* `diff`: the changes in JSOP format, limited to 2048 characters.
  A new index definition is compared with the index definition it is based on
  (an index listed in `merges`, preferably the product index, else the closest lower version of the same index),
  so that the entry shows what the new version changes. This base is stored as `diffBase`.
  The diff of a deleted index is empty: the definition is known from the earlier entries.
* `reindexRequested`: if the commit also requested a reindex

Not part of the diff are hidden properties and nodes (the index data),
and properties that are internal bookkeeping:
`reindexCount`, `seed`, `refresh`, `reindex` set to `false`, `deletedBy`,
`mergeChecksum`, `mergeInfo`, and the state of an index import.
The diff index nodes (`diff.index`, `diff.index.optimizer`) are not recorded;
the index definitions generated from them are.

Not recorded are:

* changes made by Oak itself, for example by the asynchronous index update,
  or disabling the indexes listed in `supersedes` of a new index;
* changes that are committed without the index update (for example with an empty commit hook);
* the removal of the parent node of an `oak:index` node.

### Storage

The entries are stored in the hidden node `/oak:index/:audit`, which mirrors the index paths,
for example `/oak:index/:audit/oak:index/fooIndex/20260101T1200Z.0`.
Entries are named after the minute (UTC) of the change and a sequence number.
The last 1000 entries per index are kept (system property `oak.indexDefinitionAudit.maxEntries`).

When an index definition is removed, its history is moved to `/oak:index/:audit/:deleted/<index path>`,
and moved back if an index definition with the same path is created again.
The histories of the last 1000 removed indexes are kept (system property `oak.indexDefinitionAudit.maxDeleted`).

### Index Manager MBean

The MBean of type `IndexManager` ("Index Manager") has the following operations:

* `Indexes` (attribute): all indexes with an audit trail, with the last operation, its time and user,
  and the number of entries.
* `getAuditTrail(indexPath)`: the entries of an index, newest first, also of a removed index.
  The index name alone (for example `fooIndex`) refers to `/oak:index/fooIndex`.
* `disableIndex(indexPath, by, reason, force)`: sets the type to `disabled` and stores the previous type
  in `originalType`, as well as `disabledAt`, `disabledBy` and `disabledReason`.
  Does nothing if the index is already disabled.
* `enableIndex(indexPath, by, force)`: restores the type stored in `originalType`
  (or in `:originalType`, set by the purge of old index versions with oak-run),
  removes the properties set when disabling, and sets `reindex` to `true`:
  a disabled index is not updated, and its data may have been removed.
  A synchronous index is reindexed in the same commit (that is, by this operation);
  an asynchronous index by its indexing lane.
  Does nothing if the index is not disabled.
* `deleteIndex(indexPath, by, reason, force)`: deletes a disabled index definition.
  The index data (hidden child nodes) is removed in batches first, so that indexes of any size can be deleted,
  and then the index definition, in one commit with the index update
  (so that for example the node type index no longer lists the index definition).
  If the index is enabled meanwhile, the operation fails, and the index definition is not removed.
  Index data stored outside of the repository (for example in Elasticsearch) is not removed.

Notes:

* The operations that change index definitions are only available if the audit is enabled.
  They only change index definitions directly below `/oak:index`.
* Changes are committed with the index update of the repository and with conflict handling,
  but not with the other commit hooks of the repository (as for the index importer).
* Synchronous indexes (without `async` property, for example `uuid`, `nodetype` and `reference`)
  are used by every commit. They are only disabled, enabled (which reindexes them in the same call)
  or deleted if `force` is `true`.
* `by` and `reason` are optional. `by` defaults to the principals of the JMX caller, if known,
  else to `unknown`. The values are recorded as given by the caller, they are not verified.
* The changes are recorded as described above, with the user `by` and the origin `IndexManager`.
* The operations do not check whether an index is still needed
  (for example used by queries, or kept for a possible rollback).
  This is the responsibility of the caller.

### Downgrade

Older versions of Oak do not know the hidden node `/oak:index/:audit`.
Some of them read the child nodes of `/oak:index` without skipping hidden nodes
(for example when processing diff indexes),
so before downgrading, the node `/oak:index/:audit` should be removed (for example with oak-run).

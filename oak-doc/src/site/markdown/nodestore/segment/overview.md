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

# Oak Segment Tar

* [Overview](#overview)
* [Garbage Collection](#garbage-collection)
    * [Generational Garbage Collection](#generational-garbage-collection)
    * [Estimation, Compaction and Cleanup](#estimation-compaction-cleanup)
    * [Offline Garbage Collection](#offline-garbage-collection)
    * [Online Garbage Collection](#online-garbage-collection)
        * [Monitoring the log](#monitoring-the-log)
            * [When did garbage collection start?](#when-did-garbage-collection-start)
            * [When did estimation start?](#when-did-estimation-start)
            * [Is estimation disabled?](#is-estimation-disabled)
            * [Was estimation cancelled?](#was-estimation-cancelled)
            * [When did estimation complete?](#when-did-estimation-complete)
            * [When did compaction start?](#when-did-compaction-start)
            * [Is compaction disabled?](#is-compaction-disabled)
            * [Was compaction cancelled?](#was-compaction-cancelled)
            * [When did compaction complete?](#when-did-compaction-complete)
            * [How does compaction work with concurrent writes?](#how-does-compaction-works-with-concurrent-writes)
            * [When did clean-up start?](#when-did-cleanup-start)
            * [Was cleanup cancelled?](#was-cleanup-cancelled)
            * [When did cleanup complete?](#when-did-cleanup-complete)
            * [What happened during cleanup?](#what-happened-during-cleanup)
        * [Monitoring via JMX](#monitoring-via-jmx)
            * [SegmentRevisionGarbageCollection](#SegmentRevisionGarbageCollection)
* [Tools](#tools)
    * [Backup](#backup)
    * [Restore](#restore)
    * [Check](#check)
    * [Compact](#compact)
    * [Debug](#debug)
    * [Diff](#diff)
    * [History](#history)

## <a name="overview"/> Overview

Oak Segment Tar is an Oak storage backend that stores content as various types of *records* within larger *segments*. Segments themselves are collected within *tar files* along with further auxiliary information. A *journal* is used to track the latest state of the repository. It is based on the following key principles:

  * *Immutability*. Segments are immutable, which makes is easy to cache frequently accessed segments. This also makes it less likely for programming or system errors to cause repository inconsistencies, and simplifies features like backups or master-slave clustering.

  * *Compactness*. The formatting of records is optimized for size to reduce IO costs and to fit as much content in caches as possible.

  * *Locality*. Segments are written so that related records, like a node and its immediate children, usually end up stored in the same segment. This makes tree traversals very fast and avoids most cache misses for typical clients that access more than one related node per session.

The content tree and all its revisions are stored in a collection of immutable *records* within *segments*. Each segment is identified by a UUID and typically contains a continuous subset of the content tree, for example a node with its properties and closest child nodes. Some segments might also be used to store commonly occurring property values or other shared data. Segments can be to up to 256KiB in size. See [Segments and records](records.html) for a detailed description of the segments and records. 

Segments are collectively stored in *tar files* and check-summed to ensure their integrity. Tar files also contain an index of the tar segments, the graph of segment references of all segments it contains and an index of all external binaries referenced from the segments in the tar file. See [Structure of TAR files](tar.html) for details.
 
The *journal* is a special, atomically updated file that records the state of the repository as a sequence of references to successive root node records. For crash resiliency the journal is always only updated with a new reference once the referenced record has been flushed to disk.  The most recent root node reference stored in the journal is used as the starting point for garbage collection. All content currently visible to clients must be accessible through that reference. 

Oak Segment Tar is an evolution of a [previous implementation](../segmentmk.html). Upgrading requires [migrating](../../migration.html) to the [new storage format](changes.html). 

See [Design of Oak Segment Tar](classes.html) for a high level design overview of Oak Segment Tar.   

## <a name="garbage-collection"/> Garbage Collection

Garbage Collection is the set of processes and techniques employed by Oak Segment Tar to eliminate unused persisted data, thus limiting the memory and disk footprint of the system.
Most of the operations on repository data generate a certain amount of garbage.
This garbage is a byproduct of the repository operations and consists of leftover data that is not usable by the user.
If left unchecked, this garbage would just pile up, consume disk space and pollute in-memory data structures.
To avoid this, Oak Segment Tar defines garbage collection procedures to eliminate unnecessary data.

### <a name="generational-garbage-collection"/> Generational Garbage Collection

The process implemented by Oak Segment Tar to eliminate unnecessary data is a generational garbage collection algorithm.
The idea behind this algorithm is that the system assigns a generation to every piece of data generated by the user.
A generation is just a number that is monotonically increasing.

When the system first starts, every piece of data created by the user belongs to the first generation.
When garbage collection runs, a second generation is started.
As soon as the second generation is in place, data from the first generation that is still used by the user is copied over to the second generation.
From this moment on, new data will be assigned to the second generation.
Now the system contains data from the first and the second generation, but only data from the second generation is used.
The garbage collector can now remove every piece of data from the first generation.
This removal is safe, because every piece of data that is still in use was copied to the second generation when garbage collection started.

The process of creating a new generation, migrating data to the new generation and removing an old generation is usually referred to as a "garbage collection cycle".
The system goes through many garbage collection cycles over its lifetime, where every cycle removes unused data from older generations.

### <a name="estimation-compaction-cleanup"/> Estimation, Compaction and Cleanup

While the previous section describes the idea behind garbage collection, this section introduces the building blocks on top of which garbage collection is implemented.
Oak Segment Tar splits the garbage collection process in three phases: estimation, compaction and cleanup.

Estimation is the first phase of garbage collection.
In this phase, the system estimates how much garbage is actually present in the system.
If there is not enough garbage to justify the creation of a new generation, the rest of the garbage collection process is skipped.
If the output of this phase reports that the amount of garbage is beyond a certain threshold, the system creates a new generation and goes on with the next phase.

Compaction executes after a new generation is created.
The purpose of compaction is to identify data that is currently used by the user.
Once the system has a clear picture of which pieces of data the user is currently using, everything is copied to the new generation.
This phase might be very time consuming depending on the size of the repository.
The bigger the repository, the more has to be copied to the new generation.

Cleanup is the last phase of garbage collection and kicks in as soon as compaction is done.
Once relevant data is safe in the new generation, old and unused data from a previous generation can be removed.
This phase locates outdated pieces of data from one of the oldest generations and removes it from the system.
This is the only phase where data is actually deleted and disk space is finally freed.

### <a name="offline-garbage-collection"/> Offline Garbage Collection

Offline garbage collection is the procedure followed by Oak Segment Tar to execute garbage collection by taking exclusive control of the repository.

Offline garbage collection runs as a standalone Java tool manually or semi-automatically started from the command line.
The way offline garbage collection works is simpler than the online version.
It is assumed that a human operator is in charge of deciding when offline compaction is needed.
In such a case, the human operator has to take offline - hence the name - the system using the repository and start the compaction utility from the command line.

Since offline garbage collection requires human intervention to run, the estimation phase is not executed at all.
The human operator who decides to run offline garbage collection does so because he or she decided that the garbage in the repository is exceeding some arbitrary threshold.
Since the decision comes from a human operator, offline garbage collection is not in charge of implementing heuristics to decide if and when garbage collection should be run.
The offline garbage collection process consist of the compaction and cleanup phases only.

The main drawback of offline garbage collection is that the process has to take exclusive control of the repository.
Nevertheless, this is also a strength.
Having exclusive access to the repository, offline garbage collection is usually faster and more effective of its online counterpart.
Because of this, offline garbage collection is (and will always be) an important tool in repository management.

### <a name="online-garbage-collection"/> Online Garbage Collection

Online garbage collection is the procedure followed by Oak Segment Tar to execute garbage collection on a running system.
The online garbage collection procedure aims at removing garbage with minimal interruption on the system.
Online garbage collection runs as a background process at regular intervals of time, potentially removing unused data at each iteration.
The main benefit of online garbage collection is that it runs concurrently with other system activities: it does not require the user to shut down the system for it to work.

#### <a name="monitoring-the-log"/> Monitoring the log

Online garbage collection prints lots of useful information to the system log.
This section groups those log messages by function, so to provide a useful reference to understand the different activities performed by online garbage collection.

Please note that the following messages are to be used as an example only.
To make the examples clear, some information like the date and time, the name of the thread, and the name of the logger are removed.
These information depend on the configuration of your logging framework.
Moreover, some of those messages contain data that can and will change from one execution to the other.

Every log message generated during the garbage collection process includes a sequence number 
indicating how many times garbage collection ran since the system started.
The sequence number is always printed at the beginning of the message like in the following example.

```
TarMK GC #2: ...
```

##### <a name="when-did-garbage-collection-start"/> When did garbage collection start?

As soon as garbage collection is triggered, the following message is printed.

```
TarMK GC #2: started
```

##### <a name="when-did-estimation-start"/> When did estimation start?

As soon as the estimation phase of garbage collection starts, the following message is printed.

```
TarMK GC #2: estimation started
```

##### <a name="is-estimation-disabled"/> Is estimation disabled?

The estimation phase can be disabled by configuration. If this is the case, the system prints the following message.

```
TarMK GC #2: estimation skipped because it was explicitly disabled
```

Estimation is also skipped when compaction is disabled on the system. In this case, the following message is printed instead.

```
TarMK GC #2: estimation skipped because compaction is paused
```

##### <a name="was-estimation-cancelled"/> Was estimation cancelled?

The execution of the estimation phase can be cancelled manually by the user or automatically if certain events occur.
If estimation is cancelled, the following message is printed.

```
TarMK GC #2: estimation interrupted: ${REASON}. Skipping compaction.
```

The placeholder `${REASON}` is not actually printed in the message, but will be substituted by a more specific description of the reason that brought estimation to a premature halt.
As stated before, some external events can terminate estimation, e.g. not enough memory or disk space on the host system.
Moreover, estimation can also be cancelled by shutting down the system or by explicitly cancelling it via administrative interfaces.
In each of these cases, the reason why estimation is cancelled will be printed in the log.

##### <a name="when-did-estimation-complete"/> When did estimation complete?

When estimation terminates, either because of external cancellation or after a successful execution, the following message is printed.

```
TarMK GC #2: estimation completed in 961.8 μs (0 ms). ${RESULT}
```

Moreover, the duration of the estimation phase is printed both in a readable format and in milliseconds.
The placeholder `${RESULT}` stands for a message that depends on the estimation strategy.

##### <a name="when-did-compaction-start"/> When did compaction start?

When the compaction phase of the garbage collection process starts, the following message is printed.

```
TarMK GC #2: compaction started, gc options=SegmentGCOptions{paused=false, estimationDisabled=false, gcSizeDeltaEstimation=1, retryCount=5, forceTimeout=3600, retainedGenerations=2, gcSizeDeltaEstimation=1}
```

The message includes a dump of the garbage collection options that are used during the compaction phase.

##### <a name="is-compaction-disabled"/> Is compaction disabled?

The compaction phase can be skipped by pausing the garbage collection process. If compaction is paused, the following message is printed.

```
TarMK GC #2: compaction paused
```

As long as compaction is paused, neither the estimation phase nor the compaction phase will be executed.

##### <a name="was-compaction-cancelled"/> Was compaction cancelled?

The compaction phase can be cancelled manually by the user or automatically because of external events. If compaction is cancelled, the following message is printed.

```
TarMK GC #2: compaction cancelled: ${REASON}.
```

The placeholder `${REASON}` is not actually printed in the message, but will be substituted by a more specific description of the reason that brought compaction to a premature halt.
As stated before, some external events can terminate compaction, e.g. not enough memory or disk space on the host system.
Moreover, compaction can also be cancelled by shutting down the system or by explicitly cancelling it via administrative interfaces.
In each of these cases, the reason why compaction is cancelled will be printed in the log.

##### <a name="when-did-compaction-complete"/> When did compaction complete?

When compaction complete successfully, the following message is printed.

```
TarMK GC #2: compaction succeeded in 6.580 min (394828 ms), after 2 cycles
```

The time shown in the log message is relative to the compaction phase only.
The reference to the amount of cycles spent for the compaction phase is explained in more detail below.
If compaction did not complete successfully, the following message is printed instead.

```
TarMK GC #2: compaction failed in 32.902 min (1974140 ms), after 5 cycles
```

This message doesn't mean that there was an unrecoverable error, but only that compaction gave up after a certain amount of attempts.
In case an error occurs, the following message is printed instead.

```
TarMK GC #2: compaction encountered an error
```

This message is followed by the stack trace of the exception that was caught during the compaction phase.
There is also a special message that is printed if the thread running the compaction phase is interrupted.

```
TarMK GC #2: compaction interrupted
```

##### <a name="how-does-compaction-works-with-concurrent-writes"/> How does compaction work with concurrent writes?

When compaction runs as part of online garbage collection, it has to work concurrently with the rest of the system.
This means that, while compaction tries to copy useful data to the new generation, concurrent commits to the repository are writing data to the old generation.
To cope with this, compaction tries to catch up with concurrent writes by incorporating their changes into the new generation.

When compaction first tries to setup the new generation, the following message is printed.

```
TarMK GC #2: compaction cycle 0 completed in 6.580 min (394828 ms). Compacted 3e3b35d3-2a15-43bc-a422-7bd4741d97a5.0000002a to 348b9500-0d67-46c5-a683-3ea8b0e6c21c.000012c0
```

The message shows how long it took to compact the data to the new generation.
It also prints the record identifiers of the two head states.
The head state on the left belongs to the previous generation, the one on the right to the new.

If concurrent commits are detected, compaction tries to incorporate those changes in the new generation.
In this case, the following message is printed.

```
TarMK GC #2: compaction detected concurrent commits while compacting. Compacting these commits. Cycle 1 of 5
```

This message means that a new compaction cycle is automatically started.
Compaction will try to incorporate new changes for a certain amount of cycles, where the exact amount of cycles is a configuration option.
After every compaction cycle, the following message is printed.

```
TarMK GC #2: compaction cycle 1 completed in 6.580 min (394828 ms). Compacted 4d22b170-f8b7-406b-a2fc-45bf782440ac.00000065 against 3e3b35d3-2a15-43bc-a422-7bd4741d97a5.0000002a to 72e60037-f917-499b-a476-607ea6f2735c.00000d0d
```

This message contains three record identifiers instead of two.
This is because the initial state that was being compacted evolved into a different one due to the concurrent commits.
The message makes clear that the concurrent changes referenced from the first record identifier, up to the changes referenced from the second identifier, where moved to the new generation and are now referenced from third identifier.

If the system is under heavy load and too many concurrent commits are generated, compaction might fail to catch up. In this case, a message like the following is printed.

```
TarMK GC #2: compaction gave up compacting concurrent commits after 5 cycles.
```

The message means that compaction tried to compact the repository data to the new generation for five times, but every time there were concurrent changes that prevented compaction from completion.
To prevent the system from being too overloaded with background activity, compaction stopped itself after the configured amount of cycles.

At this point the system can be configured to obtain exclusive access of the system and force compaction to complete.
This means that if compaction gave up after the configured number of cycles, it would take full control over the repository and block concurrent writes.
If the system is configured to behave this way, the following message is printed.

```
TarMK GC #2: trying to force compact remaining commits for 60 seconds. Concurrent commits to the store will be blocked.
```

If, after taking exclusive control of the repository for the specified amount of time, compaction completes successfully, the following message will be printed.

```
TarMK GC #2: compaction succeeded to force compact remaining commits after 56.7 s (56722 ms).
```

Sometimes the amount of time allocated to the compaction phase in exclusive mode is not enough.
It might happen that compaction is not able to complete its work in the allocated time.
If this happens, the following message is printed.

```
TarMK GC #2: compaction failed to force compact remaining commits after 6.580 min (394828 ms). Most likely compaction didn't get exclusive access to the store.
```

Even if compaction takes exclusive access to the repository, it can still be interrupted.
In this case, the following message is printed.

```
TarMK GC #2: compaction failed to force compact remaining commits after 6.580 min (394828 ms). Compaction was cancelled: ${REASON}.
```

The placeholder `${REASON}` will be substituted with a more detailed description of the reason why compaction was stopped.

##### <a name="when-did-cleanup-start"/> When did clean-up start?

When the cleanup phase of the garbage collection process starts, the following message is printed.

```
TarMK GC #2: cleanup started.
```

##### <a name="was-cleanup-cancelled"/> Was cleanup cancelled?

If cleanup is cancelled, the following message is printed.

```
TarMK GC #2: cleanup interrupted
```

There is no way to cancel cleanup manually.
The only time cleanup can be cancel is when shutting down the repository.

##### <a name="when-did-cleanup-complete"/> When did cleanup complete?

When cleanup completes, the following message is printed.

```
TarMK GC #2: cleanup completed in 16.23 min (974079 ms). Post cleanup size is 10.4 GB (10392082944 bytes) and space reclaimed 84.5 GB (84457663488 bytes).
```

The message includes the time the cleanup phase took to complete, both in a human readable format and in milliseconds.
Next the final size of the repository is shown, followed by the amount of space that was reclaimed during the cleanup phase.
Both the final size and the reclaimed space are shown in human readable form and in bytes.

##### <a name="what-happened-during-cleanup"/> What happened during cleanup?

The first thing cleanup does is printing out the current size of the repository with a message similar to the following.

```
TarMK GC #1: current repository size is 89.3 GB (89260786688 bytes)
```

After that, the cleanup phase will iterate through every TAR file and figure out which segments are still in use and which ones can be reclaimed.
Cleanup will print a sequence of messages like the following.

```
data00000a.tar: size of bulk references/reclaim set 0/6
```

The first part of the message is the TAR file analyzed last.
The two numbers at the end give an idea of how many references to segments are being (transitively) followed and how many of them point to bulk segments that can be removed.

After the cleanup phase scanned the repository, TAR files are purged of unused segments.
In some cases, a TAR file would end up containing no segments at all.
In this case, the TAR file is marked for deletion and the following message is printed.

```
TarMK GC #2: cleanup marking files for deletion: data00000a.tar
```

Please note that this message doesn't mean that cleanup will physically remove the file right now.
The file is only being marked as deletable.
Another background task will periodically kick in and remove unused files from disk.
When this happens, the following message is printed.

```
Removed files data00000a.tar,data00001a.tar,data00002a.tar
```

The output of this message can vary.
It depends on the amount of segments that were cleaned up, on how many TAR files were emptied and on how often the background activity removes unused files.

#### <a name="monitoring-via-jmx"/> Monitoring via JMX

The Segment Store exposes certain pieces of information via JMX.
This allows clients to easily access some statistics about the Segment Store, and connect the Segment Store to whatever monitoring infrastructure is in place.
Moreover, JMX can be useful to execute some low-level operations in a manual fashion.

##### <a name="SegmentRevisionGarbageCollection"/> SegmentRevisionGarbageCollection

The `SegmentRevisionGarbageCollection` MBean tracks statistics about garbage collection.
Some of the statistics are specific to specific phases of the garbage collection process, others are more widely applicable.
This MBean also exposes management operations to start and cancel garbage collection and options that can influence the outcome of garbage collection.
You should use this MBean with great care.

The following options are collectively called "garbage collection options", since they are used to tweak the behaviour of the garbage collection process.
These options are readable and writable, but they take effect only at the start of the next garbage collection process.

* **PausedCompaction (boolean)**
Determines if garbage collection is paused.
If this value is set to `true`, garbage collection will not be performed.
Compaction will be effectively skipped even if invoked manually or by scheduled maintenance tasks.
* **RetryCount (int)**
Determines how many completion attempts the compaction phase should try before giving up.
This parameter influences the behaviour of the compaction phase when concurrent writes are detected.
* **ForceTimeout (int)**
The amount of time (in seconds) the compaction phase can take exclusive control of the repository.
This parameter is used only if compaction is configured to take exclusive control of the repository instead of giving up after too many concurrent writes.
* **RetainedGenerations (int)**
How many generations should be preserved when cleaning up the Segment Store.
When the cleanup phase runs, only the latest `RetainedGenerations` generations are kept intact.
Older generations will be deleted.
* **GcSizeDeltaEstimation (long)**
The size (in bytes) of new content added to the repository since the end of the last garbage collection that would trigger another garbage collection run.
This parameter influences the behaviour of the estimation phase.
* **EstimationDisabled (boolean)**
Determines if the estimation phase is disabled.
If this parameter is set to `true`, the estimation phase will be skipped and compaction will run unconditionally.
* **MemoryThreshold (int)**
A number between `0` and `100` that represents the percentage of heap memory that should always be free during compaction.
If the amount of free memory falls below the provided percentage, compaction will be interrupted.

The following options are read-only and expose runtime statistics about the garbage collection process.

* **LastCompaction (string)**
The formatted timestamp of the end of the last successful compaction phase.
* **LastCleanup (string)**
The formatted timestamp of the end of the last cleanup phase.
* **LastRepositorySize (long)**
The size of the repository (in bytes) after the last cleanup phase.
* **LastReclaimedSize (long)**
The amount of data (in bytes) that was reclaimed during the last cleanup phase.
* **LastError (string)**
The last error encountered during compaction, in a human readable form.
* **LastLogMessage (string)**
The last log message produced during garbage collection.
* **Status (string)**
The current status of the garbage collection process.
This property can assume the values `idle`, `estimation`, `compaction`, `compaction-retry-N` (where `N` is the number of the current retry iteration), `compaction-force-compact` and `cleanup`.

The `SegmentRevisionGarbageCollection` MBean also exposes the following management operations.

* **cancelRevisionGC**
If garbage collection is currently running, schedule its cancellation.
The garbage collection process will be interrupted as soon as it's safe to do so without losing data or corrupting the system.
If garbage collection is not running, this operation has no effect.
* **startRevisionGC**
Start garbage collection.
If garbage collection is already running, this operation has no effect.

## <a name="tools"/> Tools

Oak Segment Tar exposes a number of command line tools that can be used to perform different tasks on the repository.

The tools are exposed as sub-commands of [Oak Run](https://github.com/apache/jackrabbit-oak/tree/trunk/oak-run).
The following sections assume that you have built this module or that you have a compiled version of it.

### <a name="backup"/> Backup

```
java -jar oak-run.jar backup ORIGINAL BACKUP 
```

The `backup` tool performs a backup of a Segment Store `ORIGINAL` and saves it to the folder `BACKUP`. 
`ORIGINAL` must be the path to an existing, valid Segment Store.
`BACKUP` must be a valid path to a folder on the file system.
If `BACKUP` doesn't exist, it will be created.
If `BACKUP` exists, it must be a path to an existing, valid Segment Store.

The tool assumes that the `ORIGINAL` Segment Store doesn't use an external Blob Store.
If this is the case, it's necessary to set the `oak.backup.UseFakeBlobStore` system property to `true` on the command line as shown below.

```
java -Doak.backup.UseFakeBlobStore=true -jar oak-run.jar backup ...
```

When a backup is performed, if `BACKUP` points to an existing Segment Store, only the content that is different from `ORIGINAL` is copied.
This is similar to an incremental backup performed at the level of the content.
When an incremental backup is performed, the tool will automatically try to cleanup eventual garbage from the `BACKUP` Segment Store.

### <a name="restore"/> Restore

```
java -jar oak-run.jar restore ORIGINAL BACKUP
```

The `restore` tool restores the state of the `ORIGINAL` Node Store from a previous backup `BACKUP`. 
This tool is the counterpart of `backup`.

### <a name="check"/> Check

```
java -jar oak-run.jar check --path PATH [--journal JOURNAL] [--deep] [--notify SECS] [--bin [LENGTH]]
```

The `check` tool inspects an existing Segment Store at `PATH` for eventual inconsistencies. 
The algorithm implemented by this tool traverses every revision in the journal, from the most recent to the oldest.
For every revision, the actual nodes and properties are traversed, verifying that every piece of data is reachable and undamaged.
  
If the `--journal` option is specified, the tool will use the journal file at `JOURNAL` instead of picking up the one contained in `PATH`. 
`JOURNAL` must be a path to a valid journal file for the Segment Store. 

If the `--deep` option is specified, the tool will perform a deep scan of the content tree, traversing every node.

If the `--notify` option is specified, the tool will print progress information messages every `SECS` seconds.
If not specified, progress information messages will be disabled.
If `SECS` equals `0`, every progress information message is printed.

If the `--bin` option is specified, the tool will scan the content of binary properties, up to the specified length `LENGTH`.
The default value for `LENGTH` is `0`, effectively disabling the traversal of binary properties.
If `LENGTH` is set to a value greater than `0`, only the initial `LENGTH` bytes of binary properties are traversed.
If `LENGTH` is set to `-1`, binary properties are fully traversed.
The `--bin` property has no effect on binary properties stored in an external Blob Store.

### <a name="compact"/> Compact

```
java -jar oak-run.jar compact PATH
```

The `compact` command performs offline compaction on the Segment Store at `PATH`. 
`PATH` must be a valid path to an existing Segment Store. 

### <a name="debug"/> Debug

```
java -jar oak-run.jar debug PATH
java -jar oak-run.jar debug PATH ITEMS...
```

The `debug` command prints diagnostic information about a Segment Store or individual Segment Store items.

`PATH` is mandatory and must be a valid path to an existing Segment Store. 
If only the path is specified - as in the first example above - only general debugging information about the Segment Store are printed.
 
`ITEMS` is a sequence of one or more TAR file name, segment ID, node record ID or range of node record ID.
If one or more items are specified - as in the second example above - general debugging information about the segment store are not printed.
Instead, detailed information about the specified items are shown.

A TAR file is specified by its name.
Every string in `ITEMS` ending in`.tar` is assumed to be a name of a TAR file.

A segment ID is specified by its UUID representation, e.g. `333dc24d-438f-4cca-8b21-3ebf67c05856`.

A node record ID is specified by a concatenation of a UUID and a record number, e.g. `333dc24d-438f-4cca-8b21-3ebf67c05856:12345`.
The record ID must point to a valid node record.
A node record ID can be optionally followed by path, like `333dc24d-438f-4cca-8b21-3ebf67c05856:12345/path/to/child`. 
When a node record ID is provided, the tool will print information about the node record pointed by it.
If a path is specified, the tool will additionally print information about every child node identified by that path.

A node record ID range is specified by a pair of record IDs separated by a hyphen (`-`), e.g. `333dc24d-438f-4cca-8b21-3ebf67c05856:12345-46116fda-7a72-4dbc-af88-a09322a7753a:67890`.
Both record IDs must point to valid node records.
The pair of record IDs can be followed by a path, like `333dc24d-438f-4cca-8b21-3ebf67c05856:12345-46116fda-7a72-4dbc-af88-a09322a7753a:67890/path/to/child`.
When a node record ID range is specified, the tool will perform a diff between the two nodes pointed by the record IDs, optionally following the provided path.
The result of the diff will be printed in JSOP format.

### <a name="diff"/> Diff

```
java -jar oak-run.jar tarmkdiff [--output OUTPUT] --list PATH
java -jar oak-run.jar tarmkdiff [--output OUTPUT] [--incremental] [--path NODE] [--ignore-snfes] --diff REVS PATH
```

The `diff` command prints content diffs between revisions in the Segment Store at `PATH`.

The `--output` option instructs the command to print its output to the file `OUTPUT`.
If this option is not specified, the tool will print to a `.log` file augmented with the current timestamp.
The default file will be saved in the current directory.

If the `--list` option is specified, the command just prints a list of revisions available in the Segment Store.
This is equivalent to the first command line specification in the example above.

If the `--list` option is not specified, `tarmkdiff` prints one or more content diff between a pair of revisions.
In this case, the command line specification is the second in the example above.

The `--diff` option specifies an interval of revisions `REVS`.
The interval is specified by a couple of revisions separated by two dots, e.g. `333dc24d-438f-4cca-8b21-3ebf67c05856:12345..46116fda-7a72-4dbc-af88-a09322a7753a:67890`.
In place of any of the two revisions, the placeholder `head` can be used.
The `head` placeholder is substituted (in a case-insensitive way) to the most recent revision in the Segment Store.
 
The `--path` option can be used to restrict the diff to a portion of the content tree.
The value `NODE` must be a valid path in the content tree.

If the flag `--incremental` is specified, the output will contain an incremental diff between every pair of successive revisions occurring in the interval specified with `--diff`.
This parameter is useful if you are interested in every change in content between every commit that happened in a specified range. 

The `--ignore-snfes` flag can be used in combination with `--incremental` to ignore errors that might occur while generating the incremental diff because of damaged or too old content.
If this flag is not specified and an error occurs while generating the incremental diff, the tool stops immediately and reports the error.

### <a name="history"/> History

```
java -jar oak-run.jar history [--journal JOURNAL] [--path NODE] [--depth DEPTH] PATH
```

The `history` command shows how the content of a node or of a sub-tree changed over time in the Segment Store at `PATH`.

The history of the node is computed based on the revisions reported by the journal in the Segment Store.
If a different set of revisions needs to be used, it is possible to specify a custom journal file by using the `--journal` option.
If this option is used, `JOURNAL` must be a path to a valid journal file.

The `--path` parameter specifies the node whose history will be printed. 
If not specified, the history of the root node will be printed.
`NODE` must be a valid path to a node in the Segment Store.

The `--depth` parameter determines if the content of a single node should be printed, or if the content of the sub-tree rooted at that node should be printed instead.
`DEPTH` must be a positive integer specifying how deep the printed content should be.
If this option is not specified, the depth is assumed to be `0`, i.e. only information about the node will be printed.


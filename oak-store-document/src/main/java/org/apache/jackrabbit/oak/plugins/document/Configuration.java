/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.document;

import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.osgi.service.metatype.annotations.Option;

import static org.apache.jackrabbit.oak.plugins.document.Configuration.PID;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_CACHE_SEGMENT_COUNT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_CACHE_STACK_MOVE_DISTANCE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_NODE_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT;

@ObjectClassDefinition(
        pid = {PID},
        name = "Apache Jackrabbit Oak Document NodeStore Service",
        description = "NodeStore implementation based on Document model. For configuration option refer " +
                "to http://jackrabbit.apache.org/oak/docs/osgi_config.html#DocumentNodeStore. Note that for system " +
                "stability purpose it is advisable to not change these settings at runtime. Instead the config change " +
                "should be done via file system based config file and this view should ONLY be used to determine which " +
                "options are supported")
@interface Configuration {

    String PID = "org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService";

    String PRESET_PID = PID + "Preset";

    @AttributeDefinition(
            name = "Mongo URI",
            description = "Mongo connection URI used to connect to Mongo. Refer to " +
                    "http://docs.mongodb.org/manual/reference/connection-string/ for details. Note that this value " +
                    "can be overridden via framework property 'oak.mongo.uri'")
    String mongouri() default DocumentNodeStoreService.DEFAULT_URI;

    @AttributeDefinition(
            name = "Mongo DB name",
            description = "Name of the database in Mongo. Note that this value " +
                    "can be overridden via framework property 'oak.mongo.db'")
    String db() default DocumentNodeStoreService.DEFAULT_DB;


    @AttributeDefinition(
            name = "MongoDB socket keep-alive option",
            description = "Whether socket keep-alive should be enabled for " +
                    "connections to MongoDB. Note that this value can be " +
                    "overridden via framework property 'oak.mongo.socketKeepAlive'")
    boolean socketKeepAlive() default DocumentNodeStoreService.DEFAULT_SO_KEEP_ALIVE;

    @AttributeDefinition(
            name = "Cache Size (in MB)",
            description = "Cache size in MB. This is distributed among various caches used in DocumentNodeStore")
    int cache() default DocumentNodeStoreService.DEFAULT_CACHE;

    @AttributeDefinition(
            name = "NodeState Cache",
            description = "Percentage of cache to be allocated towards Node cache")
    int nodeCachePercentage() default DEFAULT_NODE_CACHE_PERCENTAGE;

    @AttributeDefinition(
            name = "PreviousDocument Cache",
            description = "Percentage of cache to be allocated towards Previous Document cache")
    int prevDocCachePercentage() default DEFAULT_PREV_DOC_CACHE_PERCENTAGE;

    @AttributeDefinition(
            name = "NodeState Children Cache",
            description = "Percentage of cache to be allocated towards Children cache")
    int childrenCachePercentage() default DEFAULT_CHILDREN_CACHE_PERCENTAGE;

    @AttributeDefinition(
            name = "Diff Cache",
            description = "Percentage of cache to be allocated towards Diff cache")
    int diffCachePercentage() default DEFAULT_DIFF_CACHE_PERCENTAGE;

    @AttributeDefinition(
            name = "LIRS Cache Segment Count",
            description = "The number of segments in the LIRS cache " +
                    "(default 16, a higher count means higher concurrency " +
                    "but slightly lower cache hit rate)")
    int cacheSegmentCount() default DEFAULT_CACHE_SEGMENT_COUNT;

    @AttributeDefinition(
            name = "LIRS Cache Stack Move Distance",
            description = "The delay to move entries to the head of the queue " +
                    "in the LIRS cache " +
                    "(default 16, a higher value means higher concurrency " +
                    "but slightly lower cache hit rate)")
    int cacheStackMoveDistance() default DEFAULT_CACHE_STACK_MOVE_DISTANCE;

    @AttributeDefinition(
            name = "Blob Cache Size (in MB)",
            description = "Cache size to store blobs in memory. Used only with default BlobStore " +
                    "(as per DocumentStore type)")
    int blobCacheSize() default DocumentNodeStoreService.DEFAULT_BLOB_CACHE_SIZE;

    @AttributeDefinition(
            name = "Persistent Cache Config",
            description = "Configuration for persistent cache. Refer to " +
                    "http://jackrabbit.apache.org/oak/docs/nodestore/persistent-cache.html for various options")
    String persistentCache() default DocumentNodeStoreService.DEFAULT_PERSISTENT_CACHE;

    @AttributeDefinition(
            name = "Journal Cache Config",
            description = "Configuration for journal cache. Refer to " +
                    "http://jackrabbit.apache.org/oak/docs/nodestore/persistent-cache.html for various options")
    String journalCache() default DocumentNodeStoreService.DEFAULT_JOURNAL_CACHE;

    @AttributeDefinition(
            name = "Custom BlobStore",
            description = "Boolean value indicating that a custom BlobStore is to be used. " +
                    "By default, for MongoDB, MongoBlobStore is used; for RDB, RDBBlobStore is used.")
    boolean customBlobStore() default DocumentNodeStoreService.DEFAULT_CUSTOM_BLOB_STORE;


    @AttributeDefinition(
            name = "Journal Garbage Collection Interval (millis)",
            description = "Long value indicating interval (in milliseconds) with which the "
                    + "journal (for external changes) is cleaned up. Default is " + DocumentNodeStoreService.DEFAULT_JOURNAL_GC_INTERVAL_MILLIS)
    long journalGCInterval() default DocumentNodeStoreService.DEFAULT_JOURNAL_GC_INTERVAL_MILLIS;

    @AttributeDefinition(
            name = "Maximum Age of Journal Entries (millis)",
            description = "Long value indicating max age (in milliseconds) that "
                    + "journal (for external changes) entries are kept (older ones are candidates for gc). "
                    + "Default is " + DocumentNodeStoreService.DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS)
    long journalGCMaxAge() default DocumentNodeStoreService.DEFAULT_JOURNAL_GC_MAX_AGE_MILLIS;

    @AttributeDefinition(
            name = "Pre-fetch external changes",
            description = "Boolean value indicating if external changes should " +
                    "be pre-fetched in a background thread.")
    boolean prefetchExternalChanges() default DocumentNodeStoreService.DEFAULT_PREFETCH_EXTERNAL_CHANGES;

    @AttributeDefinition(
            name = "NodeStoreProvider role",
            description = "Property indicating that this component will not register as a NodeStore but as a " +
                    "NodeStoreProvider with given role")
    String role();

    @AttributeDefinition(
            name = "Version GC Max Age (in secs)",
            description = "Version Garbage Collector (GC) logic will only consider those deleted for GC which " +
                    "are not accessed recently (currentTime - lastModifiedTime > versionGcMaxAgeInSecs). For " +
                    "example as per default only those document which have been *marked* deleted 24 hrs ago will be " +
                    "considered for GC. This also applies how older revision of live document are GC.")
    long versionGcMaxAgeInSecs() default DocumentNodeStoreService.DEFAULT_VER_GC_MAX_AGE;

    @AttributeDefinition(
            name = "Version GC scheduler expression",
            description = "A cron expression that defines when the Version GC is scheduled. " +
                    "If this configuration entry is left empty, the default behaviour depends on " +
                    "the 'documentStoreType'. For 'MONGO' the default is to schedule a " +
                    "run every five seconds (also known as Continuous Revision Garbage " +
                    "Collection). For 'RDB' the default is no scheduled GC. It must be " +
                    "enabled explicitly with a cron expression. E.g. the following " +
                    "expression triggers a GC run every night at 2 AM: '" +
                    DocumentNodeStoreService.CLASSIC_RGC_EXPR + "'."
    )
    String versionGCExpression() default DocumentNodeStoreService.DEFAULT_VER_GC_EXPRESSION;

    @AttributeDefinition(
            name = "Time limit for a Version GC run (in sec)",
            description = "A Version GC run is canceled after this number of seconds. " +
                    "The default value is " + DocumentNodeStoreService.DEFAULT_RGC_TIME_LIMIT_SECS +
                    " seconds.")
    long versionGCTimeLimitInSecs() default DocumentNodeStoreService.DEFAULT_RGC_TIME_LIMIT_SECS;

    @AttributeDefinition(
            name = "Blob GC Max Age (in secs)",
            description = "Blob Garbage Collector (GC) logic will only consider those blobs for GC which " +
                    "are not accessed recently (currentTime - lastModifiedTime > blobGcMaxAgeInSecs). For " +
                    "example as per default only those blobs which have been created 24 hrs ago will be " +
                    "considered for GC")
    long blobGcMaxAgeInSecs() default DocumentNodeStoreService.DEFAULT_BLOB_GC_MAX_AGE;

    @AttributeDefinition(
            name = "Blob tracking snapshot interval (in secs)",
            description = "This is the default interval in which the snapshots of locally tracked blob ids will"
                    + "be taken and synchronized with the blob store. This should be configured to be less than the "
                    + "frequency of blob GC so that deletions during blob GC can be accounted for "
                    + "in the next GC execution.")
    long blobTrackSnapshotIntervalInSecs() default DocumentNodeStoreService.DEFAULT_BLOB_SNAPSHOT_INTERVAL;

    @AttributeDefinition(
            name = "Root directory",
            description = "Root directory for local tracking of blob ids. This service " +
                    "will first lookup the 'repository.home' framework property and " +
                    "then a component context property with the same name. If none " +
                    "of them is defined, a sub directory 'repository' relative to " +
                    "the current working directory is used.")
    String repository_home();

    @AttributeDefinition(
            name = "Max Replication Lag (in secs)",
            description = "Value in seconds. Determines the duration beyond which it can be safely assumed " +
                    "that the state on the secondaries is consistent with the primary, and it is safe to read from them")
    long maxReplicationLagInSecs() default DocumentNodeStoreService.DEFAULT_MAX_REPLICATION_LAG;

    @AttributeDefinition(
            name = "DocumentStore Type",
            description = "Type of DocumentStore to use for persistence. Defaults to MONGO",
            options = {
                    @Option(label = "MONGO", value = "MONGO"),
                    @Option(label = "RDB", value = "RDB")})
    String documentStoreType() default "MONGO";

    @AttributeDefinition(
            name = "Bundling Disabled",
            description = "Boolean value indicating that Node bundling is disabled")
    boolean bundlingDisabled() default DocumentNodeStoreService.DEFAULT_BUNDLING_DISABLED;

    @AttributeDefinition(
            name = "DocumentNodeStore update.limit",
            description = "Number of content updates that need to happen before " +
                    "the updates are automatically purged to the private branch.")
    int updateLimit() default DEFAULT_UPDATE_LIMIT;

    @AttributeDefinition(
            name = "Persistent Cache Includes",
            description = "Paths which should be cached in persistent cache")
    String[] persistentCacheIncludes() default {"/"};
}

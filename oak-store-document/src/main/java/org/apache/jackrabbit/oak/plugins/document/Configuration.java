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

import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_RECOVERY_DELAY_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.CommitQueue.DEFAULT_SUSPEND_TIMEOUT;
import static org.apache.jackrabbit.oak.plugins.document.ClusterNodeInfo.DEFAULT_REUSE_DELAY_AFTER_RECOVERY_MILLIS;
import static org.apache.jackrabbit.oak.plugins.document.Configuration.PID;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_CACHE_SEGMENT_COUNT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_CACHE_STACK_MOVE_DISTANCE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_CHILDREN_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_DIFF_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_NODE_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_PREV_DOC_CACHE_PERCENTAGE;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreBuilder.DEFAULT_UPDATE_LIMIT;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FULL_GC_ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_EMBEDDED_VERIFICATION_ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_THROTTLING_ENABLED;
import static org.apache.jackrabbit.oak.plugins.document.DocumentNodeStoreService.DEFAULT_FULL_GC_MODE;

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
            name = "MongoDB socket timeout for lease update operations",
            description = "Socket timeout for lease update operations in " +
                    "milliseconds. Note that this value can be " +
                    "overridden via framework property 'oak.mongo.leaseSocketTimeout'")
    int mongoLeaseSocketTimeout() default DocumentNodeStoreService.DEFAULT_MONGO_LEASE_SO_TIMEOUT_MILLIS;

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
                    "If this configuration entry is left empty, the default behaviour is to " +
                    "schedule a run every five seconds (also known as Continuous Revision Garbage " +
                    "Collection). Otherwise, the schedule can be configured with a cron " +
                    "expression. E.g. the following expression triggers a GC run every night at 2 AM: '" +
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
            name = "Delay factor for a Version GC run",
            description = "A Version GC run has a gap of this delay factor to reduce continuous load on system" +
                    "The default value is " + DocumentNodeStoreService.DEFAULT_RGC_DELAY_FACTOR)
    double versionGCDelayFactor() default DocumentNodeStoreService.DEFAULT_RGC_DELAY_FACTOR;

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
            description = "Paths which should be cached in persistent cache. " +
                    "This value can be overridden with a system property " +
                    "'oak.documentstore.persistentCacheIncludes' where paths " +
                    "are separated with '::'. Example: -Doak.documentstore.persistentCacheIncludes=/content::/var")
    String[] persistentCacheIncludes() default {"/"};

    @AttributeDefinition(
            name = "Full GC Include Paths",
            description = "Paths which should be included in full garbage collection. " +
                    "Include and exclude paths can overlap. Exclude paths will take precedence. " +
                    "Note that this value can be overridden with a system property " +
                    "'oak.documentstore.fullGCIncludePaths' where paths " +
                    "are separated with '::'. Example: -Doak.documentstore.fullGCIncludePaths=/content::/var")
    String[] fullGCIncludePaths() default {"/"};

    @AttributeDefinition(
            name = "Full GC Exclude Paths",
            description = "Paths which should be excluded from full Garbage collection. " +
                    "Include and exclude paths can overlap. Exclude paths will take precedence. " +
                    "Note that this value can be overridden with a system property " +
                    "'oak.documentstore.fullGCExcludePaths' where paths " +
                    "are separated with '::'. Example: -Doak.documentstore.fullGCExcludePaths=/content::/var")
    String[] fullGCExcludePaths() default {};

    @AttributeDefinition(
            name = "Lease check mode",
            description = "The lease check mode. 'STRICT' is the default and " +
                    "will stop the DocumentNodeStore as soon as the lease " +
                    "expires. 'LENIENT' will give the background lease update " +
                    "a chance to renew the lease even when the lease expired. " +
                    "This mode is only recommended for development, e.g. when " +
                    "debugging an application and the lease may expire when " +
                    "the JVM is stopped at a breakpoint.",
            options = {
            @Option(label = "STRICT", value = "STRICT"),
            @Option(label = "LENIENT", value = "LENIENT")})
    String leaseCheckMode() default "STRICT";

    @AttributeDefinition(
            name = "Document Node Store throttling",
            description = "Boolean value indicating whether throttling should be enabled for " +
                    "document node store or not. The Default value is " + DEFAULT_THROTTLING_ENABLED +
                    ". Note that this value can be overridden via framework " +
                    "property 'oak.documentstore.throttlingEnabled'")
    boolean throttlingEnabled() default DEFAULT_THROTTLING_ENABLED;

    @AttributeDefinition(
            name = "Document Node Store Compression",
            description = "Select compressor type for collections. 'Snappy' is the default supported compression.")
    String collectionCompressionType() default "snappy";

    @AttributeDefinition(
            name = "Commit Suspend timeout",
            description = "Timeout for a suspended commit after it conflicted with" +
                    "a change that is not yet visible. Default: " + DEFAULT_SUSPEND_TIMEOUT +
                    " (milliseconds).")
    long suspendTimeoutMillis() default DEFAULT_SUSPEND_TIMEOUT;

    @AttributeDefinition(
            name = "Recovery delay",
            description = "Delay (in milliseconds) before a recovery is done, " +
                    "0 or negative for no delay. Default: " + DEFAULT_RECOVERY_DELAY_MILLIS +
                    " (milliseconds).")
    long recoveryDelayMillis() default DEFAULT_RECOVERY_DELAY_MILLIS;

    @AttributeDefinition(
            name = "ClusterId reuse delay after recovery",
            description = "Minimal delay (in milliseconds) before a clusterId " +
                    "can be reused after a recovery, 0 or negative for no delay. Default: " + DEFAULT_REUSE_DELAY_AFTER_RECOVERY_MILLIS +
                    " (milliseconds).")
    long clusterIdReuseDelayAfterRecoveryMillis() default DEFAULT_REUSE_DELAY_AFTER_RECOVERY_MILLIS;

    @AttributeDefinition(
            name = "Document Node Store Full GC",
            description = "Boolean value indicating whether Full GC should be enabled for " +
                    "document node store or not. The Default value is " + DEFAULT_FULL_GC_ENABLED +
                    ". Note that this value can be overridden via framework " +
                    "property 'oak.documentstore.fullGCEnabled'")
    boolean fullGCEnabled() default DEFAULT_FULL_GC_ENABLED;

    @AttributeDefinition(
            name = "Document Node Store Embedded Verification for Full GC",
            description = "Boolean value indicating whether Embedded Verification (i.e. verify the document after " +
                    "applying changes in memory before any database calls) for Full GC should be enabled for " +
                    "document node store or not. The Default value is " + DEFAULT_EMBEDDED_VERIFICATION_ENABLED +
                    ". Note that this value can be overridden via framework " +
                    "property 'oak.documentstore.embeddedVerificationEnabled'")
    boolean embeddedVerificationEnabled() default DEFAULT_EMBEDDED_VERIFICATION_ENABLED;

    @AttributeDefinition(
            name = "Document Node Store Full GC Mode",
            description = "Integer value indicating which Full GC mode should be enabled for " +
                    "document node store. The Default value is " + DEFAULT_FULL_GC_MODE +
                    ". Note that this value can be overridden via framework " +
                    "property 'oak.documentstore.fullGCMode'. " +
                    "FullGC can be entirely enabled / disabled with the variable fullGCEnabled, unless fullGCEnabled " +
                    "is set to true, the fullGCMode will be ignored.")
    int fullGCMode() default DEFAULT_FULL_GC_MODE;

    @AttributeDefinition(
            name = "Delay factor for a Full GC run",
            description = "A Full GC run has a gap of this delay factor to reduce continuous load on system." +
                    "It allows the FullGC thread to stop by (fullGC batch run time * delayFactor) period after each batch." +
                    "The default value is " + DocumentNodeStoreService.DEFAULT_FGC_DELAY_FACTOR)
    double fullGCDelayFactor() default DocumentNodeStoreService.DEFAULT_FGC_DELAY_FACTOR;

    @AttributeDefinition(
            name = "Batch Size to fetch data for each FullGC cycle",
            description = "Integer value indicating the number of documents to fetch from database in a single query to check for Full GC." +
                    "It should be a factor of fullGCProgressSize for better performance " +
                    "The default value is " + DocumentNodeStoreService.DEFAULT_FGC_BATCH_SIZE)
    int fullGCBatchSize() default DocumentNodeStoreService.DEFAULT_FGC_BATCH_SIZE;

    @AttributeDefinition(
            name = "Progress Size for FullGC cycle",
            description = "Integer value indicating the number of documents to check for garbage in each Full GC cycle." +
                    "The default value is " + DocumentNodeStoreService.DEFAULT_FGC_PROGRESS_SIZE)
    int fullGCProgressSize() default DocumentNodeStoreService.DEFAULT_FGC_PROGRESS_SIZE;
}

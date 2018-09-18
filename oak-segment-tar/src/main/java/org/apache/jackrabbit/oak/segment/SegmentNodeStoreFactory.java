/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.segment;

import static com.google.common.base.Strings.isNullOrEmpty;
import static org.apache.jackrabbit.oak.commons.IOUtils.closeQuietly;
import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_STRING_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.CachingSegmentReader.DEFAULT_TEMPLATE_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.SegmentCache.DEFAULT_SEGMENT_CACHE_MB;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_NODE_CACHE_SIZE_OSGi;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_STRING_CACHE_SIZE_OSGi;
import static org.apache.jackrabbit.oak.segment.WriterCacheManager.DEFAULT_TEMPLATE_CACHE_SIZE_OSGi;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.DISABLE_ESTIMATION_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.FORCE_TIMEOUT_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.GC_PROGRESS_LOG_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.MEMORY_THRESHOLD_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.PAUSE_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.RETAINED_GENERATIONS_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.RETRY_COUNT_DEFAULT;
import static org.apache.jackrabbit.oak.segment.compaction.SegmentGCOptions.SIZE_DELTA_ESTIMATION_DEFAULT;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.DEFAULT_MAX_FILE_SIZE;
import static org.apache.jackrabbit.oak.spi.blob.osgi.SplitBlobStoreService.ONLY_STANDALONE_TARGET;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.google.common.io.Closer;
import org.apache.jackrabbit.api.stats.RepositoryStatistics;
import org.apache.jackrabbit.api.stats.TimeSeries;
import org.apache.jackrabbit.oak.osgi.OsgiUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;
import org.apache.jackrabbit.oak.spi.blob.BlobStore;
import org.apache.jackrabbit.oak.spi.state.NodeStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.stats.CounterStats;
import org.apache.jackrabbit.oak.stats.HistogramStats;
import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.ComponentContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.ReferencePolicyOption;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A factory allowing creation of secondary segment node stores.
 * <p>
 * The different secondaries are distinguished by their role attribute.
 */
@Component(configurationPolicy = ConfigurationPolicy.REQUIRE)
@Designate(factory = true, ocd = SegmentNodeStoreFactory.Configuration.class)
public class SegmentNodeStoreFactory {

    private final Logger log = LoggerFactory.getLogger(getClass());

    private static final long DEFAULT_BLOB_SNAPSHOT_INTERVAL = 12 * 60 * 60;

    private static final long DEFAULT_BLOB_GC_MAX_AGE = 24 * 60 * 60;

    @ObjectClassDefinition(
        name = "Apache Jackrabbit Oak Segment-Tar NodeStore Factory",
        description = "Factory allowing configuration of adjacent instances of " +
            "NodeStore implementation based on Segment model besides a default SegmentNodeStore in same setup."
    )
    @interface Configuration {

        @AttributeDefinition(
            name = "Repository Home Directory",
            description = "Path on the file system where repository data will be stored. "
                + "Defaults to the value of the framework property 'repository.home' or to 'repository' "
                + "if that is neither specified."
        )
        String repository_home() default "";

        @AttributeDefinition(
            name = "Mode",
            description = "TarMK mode (64 for memory mapped file access, 32 for normal file access). " +
                "Default value is taken from the 'sun.arch.data.model' system property."
        )
        String tarmk_mode() default "";

        @AttributeDefinition(
            name = "Maximum tar file size (MB)",
            description = "The maximum size of the tar files in megabytes. " +
                "Default value is '" + DEFAULT_MAX_FILE_SIZE + "'."
        )
        int tarmk_size() default DEFAULT_MAX_FILE_SIZE;

        @AttributeDefinition(
            name = "Segment cache size (MB)",
            description = "Cache size for storing most recently used segments in megabytes. " +
                "Default value is '" + DEFAULT_SEGMENT_CACHE_MB + "'."
        )
        int segmentCache_size() default DEFAULT_SEGMENT_CACHE_MB;

        @AttributeDefinition(
            name = "String cache size (MB)",
            description = "Cache size for storing most recently used strings in megabytes. " +
                "Default value is '" + DEFAULT_STRING_CACHE_MB + "'."
        )
        int stringCache_size() default DEFAULT_STRING_CACHE_MB;

        @AttributeDefinition(
            name = "Template cache size (MB)",
            description = "Cache size for storing most recently used templates in megabytes. " +
                "Default value is '" + DEFAULT_TEMPLATE_CACHE_MB + "'."
        )
        int templateCache_size() default DEFAULT_TEMPLATE_CACHE_MB;

        @AttributeDefinition(
            name = "String deduplication cache size (#items)",
            description = "Maximum number of strings to keep in the deduplication cache. " +
                "Default value is '" + DEFAULT_STRING_CACHE_SIZE_OSGi + "'."
        )
        int stringDeduplicationCache_size() default DEFAULT_STRING_CACHE_SIZE_OSGi;

        @AttributeDefinition(
            name = "Template deduplication cache size (#items)",
            description = "Maximum number of templates to keep in the deduplication cache. " +
                "Default value is '" + DEFAULT_TEMPLATE_CACHE_SIZE_OSGi + "'."
        )
        int templateDeduplicationCache_size() default DEFAULT_TEMPLATE_CACHE_SIZE_OSGi;

        @AttributeDefinition(
            name = "Node deduplication cache size (#items)",
            description = "Maximum number of node to keep in the deduplication cache. If the supplied " +
                "value is not a power of 2, it will be rounded up to the next power of 2. " +
                "Default value is '" + DEFAULT_NODE_CACHE_SIZE_OSGi + "'."
        )
        int nodeDeduplicationCache_size() default DEFAULT_NODE_CACHE_SIZE_OSGi;

        @AttributeDefinition(
            name = "Pause compaction",
            description = "When set to true the compaction phase is skipped during garbage collection. " +
                "Default value is '" + PAUSE_DEFAULT + "'."
        )
        boolean pauseCompaction() default PAUSE_DEFAULT;

        @AttributeDefinition(
            name = "Compaction retries",
            description = "Number of tries to compact concurrent commits on top of already " +
                "compacted commits. " +
                "Default value is '" + RETRY_COUNT_DEFAULT + "'."
        )
        int compaction_retryCount() default RETRY_COUNT_DEFAULT;

        @AttributeDefinition(
            name = "Force compaction timeout",
            description = "Number of seconds to attempt to force compact concurrent commits on top " +
                "of already compacted commits after the maximum number of retries has been " +
                "reached. Forced compaction tries to acquire an exclusive write lock on the " +
                "node store, blocking concurrent write access as long as the lock is held. " +
                "Default value is '" + FORCE_TIMEOUT_DEFAULT + "'."
        )
        int compaction_force_timeout() default FORCE_TIMEOUT_DEFAULT;

        @AttributeDefinition(
            name = "Garbage collection repository size threshold",
            description = "Garbage collection will be skipped unless the repository grew at least by " +
                "the number of bytes specified. " +
                "Default value is '" + SIZE_DELTA_ESTIMATION_DEFAULT + "'."
        )
        long compaction_sizeDeltaEstimation() default SIZE_DELTA_ESTIMATION_DEFAULT;

        @AttributeDefinition(
            name = "Disable estimation phase",
            description = "Disables the estimation phase allowing garbage collection to run unconditionally. " +
                "Default value is '" + DISABLE_ESTIMATION_DEFAULT + "'."
        )
        boolean compaction_disableEstimation() default DISABLE_ESTIMATION_DEFAULT;

        @AttributeDefinition(
            name = "Compaction retained generations",
            description = "Number of segment generations to retain during garbage collection. " +
                "The number of generations defaults to " + RETAINED_GENERATIONS_DEFAULT + " and " +
                "can't be changed. This configuration option is considered deprecated " +
                "and will be removed in the future."
        )
        int compaction_retainedGenerations() default RETAINED_GENERATIONS_DEFAULT;

        @AttributeDefinition(
            name = "Compaction memory threshold",
            description = "Threshold of available heap memory in percent of total heap memory below " +
                "which the compaction phase is canceled. 0 disables heap memory monitoring. " +
                "Default value is '" + MEMORY_THRESHOLD_DEFAULT + "'."
        )
        int compaction_memoryThreshold() default MEMORY_THRESHOLD_DEFAULT;

        @AttributeDefinition(
            name = "Compaction progress log",
            description = "The number of nodes compacted after which a status message is logged. " +
                "-1 disables progress logging. " +
                "Default value is '" + GC_PROGRESS_LOG_DEFAULT + "'."
        )
        long compaction_progressLog() default GC_PROGRESS_LOG_DEFAULT;

        @AttributeDefinition(
            name = "Standby mode",
            description = "Flag indicating this component will not register as a NodeStore but as a " +
                "NodeStoreProvider instead. " +
                "Default value is 'false'."
        )
        boolean standby() default false;

        @AttributeDefinition(
            name = "Custom blob store",
            description = "Boolean value indicating that a custom BlobStore is used for storing " +
                "large binary values."
        )
        boolean customBlobStore() default false;

        @AttributeDefinition(
            name = "Custom segment store",
            description = "Boolean value indicating that a custom (non-tar) segment store is used"
        )
        boolean customSegmentStore() default false;

        @AttributeDefinition(
                name = "Split persistence",
                description = "Boolean value indicating that the writes should be done locally when using the custom segment store"
        )
        boolean splitPersistence() default false;

        @AttributeDefinition(
            name = "Backup directory",
            description = "Directory (relative to current working directory) for storing repository backups. " +
                "Defaults to 'repository.home/segmentstore-backup'."
        )
        String repository_backup_dir() default "";

        @AttributeDefinition(
            name = "Blob gc max age (in secs)",
            description = "The blob garbage collection logic will only consider those blobs which " +
                "are not accessed recently (currentTime - lastModifiedTime > blobGcMaxAgeInSecs). " +
                "For example with the default setting only those blobs which have been created " +
                "at least 24 hours ago will be considered for garbage collection. " +
                "Default value is '" + DEFAULT_BLOB_GC_MAX_AGE + "'."
        )
        long blobGcMaxAgeInSecs() default DEFAULT_BLOB_GC_MAX_AGE;

        @AttributeDefinition(
            name = "Blob tracking snapshot interval",
            description = "Interval in seconds in which snapshots of locally tracked blob ids are " +
                "taken and synchronized with the blob store. This should be configured to be " +
                "less than the frequency of blob garbage collection so that deletions during blob " +
                "garbage collection can be accounted for in the next garbage collection execution. " +
                "Default value is '" + DEFAULT_BLOB_SNAPSHOT_INTERVAL + "'."
        )
        long blobTrackSnapshotIntervalInSecs() default DEFAULT_BLOB_SNAPSHOT_INTERVAL;

        @AttributeDefinition(
            name = "Role",
            description = "As multiple SegmentNodeStores can be configured, this parameter defines the role " +
                "of 'this' SegmentNodeStore."
        )
        String role() default "";

        @AttributeDefinition(
            name = "Register JCR descriptors as OSGi services",
            description = "Should only be done for one factory instance"
        )
        boolean registerDescriptors() default false;
    }

    @Reference(
        cardinality = ReferenceCardinality.OPTIONAL,
        policy = ReferencePolicy.STATIC,
        policyOption = ReferencePolicyOption.GREEDY,
        target = ONLY_STANDALONE_TARGET
    )
    private volatile BlobStore blobStore;

    @Reference(
        cardinality = ReferenceCardinality.OPTIONAL,
        policy = ReferencePolicy.STATIC,
        policyOption = ReferencePolicyOption.GREEDY
    )
    private volatile SegmentNodeStorePersistence segmentStore;

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    private final Closer registrations = Closer.create();

    @Activate
    public void activate(ComponentContext context, Configuration configuration) throws IOException {
        log.info("activate: SegmentNodeStore '" + configuration.role() + "' starting.");

        if (configuration.role().isEmpty()) {
            return;
        }

        OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());
        SegmentNodeStore store = registerSegmentStore(
            context,
            configuration,
            blobStore,
            segmentStore,
            getRoleStatisticsProvider(statisticsProvider, configuration.role()),
            registrations,
            whiteboard,
            configuration.role(),
            log
        );

        if (store == null) {
            return;
        }

        Map<String, Object> props = new HashMap<String, Object>();
        props.put(NodeStoreProvider.ROLE, configuration.role());
        registrations.register(asCloseable(whiteboard.register(NodeStoreProvider.class, () -> store, props)));
        log.info("Registered NodeStoreProvider backed by SegmentNodeStore of type '{}'", configuration.role());
    }

    @Deactivate
    public void deactivate() {
        closeQuietly(registrations);
    }

    private static Closeable asCloseable(final Registration r) {
        return new Closeable() {

            @Override
            public void close() {
                r.unregister();
            }

        };
    }

    static String property(String name, ComponentContext context) {
        return lookupConfigurationThenFramework(context, name);
    }

    private static SegmentNodeStore registerSegmentStore(
        ComponentContext context,
        Configuration configuration,
        BlobStore blobStore,
        SegmentNodeStorePersistence segmentStore,
        StatisticsProvider statisticsProvider,
        Closer closer,
        Whiteboard whiteboard,
        String role,
        Logger logger
    ) throws IOException {
        return SegmentNodeStoreRegistrar.registerSegmentNodeStore(new SegmentNodeStoreRegistrar.Configuration() {

            String appendRole(String name) {
                return name + "-" + role;
            }

            int roundToNextPowerOfTwo(int size) {
                return 1 << (32 - Integer.numberOfLeadingZeros(Math.max(0, size - 1)));
            }

            String getMode() {
                String mode = configuration.tarmk_mode();
                if (isNullOrEmpty(mode)) {
                    return System.getProperty("tarmk.mode", System.getProperty("sun.arch.data.model", "32"));
                }
                return mode;
            }

            int getCacheSize(String name, int otherwise) {
                Integer size = Integer.getInteger(name);
                if (size != null) {
                    return size;
                }
                return otherwise;
            }

            @Override
            public boolean isPrimarySegmentStore() {
                return false;
            }

            @Override
            public boolean isSecondarySegmentStore() {
                return "secondary".equals(role);
            }

            @Override
            public boolean isStandbyInstance() {
                return configuration.standby();
            }

            @Override
            public String getRole() {
                return role;
            }

            @Override
            public int getRetainedGenerations() {
                return configuration.compaction_retainedGenerations();
            }

            @Override
            public int getDefaultRetainedGenerations() {
                return RETAINED_GENERATIONS_DEFAULT;
            }

            @Override
            public boolean getPauseCompaction() {
                return configuration.pauseCompaction();
            }

            @Override
            public int getRetryCount() {
                return configuration.compaction_retryCount();
            }

            @Override
            public int getForceCompactionTimeout() {
                return configuration.compaction_force_timeout();
            }

            @Override
            public long getSizeDeltaEstimation() {
                return configuration.compaction_sizeDeltaEstimation();
            }

            @Override
            public int getMemoryThreshold() {
                return configuration.compaction_memoryThreshold();
            }

            @Override
            public boolean getDisableEstimation() {
                return configuration.compaction_disableEstimation();
            }

            @Override
            public long getGCProcessLog() {
                return configuration.compaction_progressLog();
            }

            @Override
            public File getSegmentDirectory() {
                return new File(getRepositoryHome(), appendRole("segmentstore"));
            }

            @Override
            public File getSplitPersistenceDirectory() {
                return new File(getRepositoryHome(), appendRole("segmentstore-split"));
            }

            @Override
            public int getSegmentCacheSize() {
                return getCacheSize("segmentCache.size", configuration.segmentCache_size());
            }

            @Override
            public int getStringCacheSize() {
                return getCacheSize("stringCache.size", configuration.stringCache_size());
            }

            @Override
            public int getTemplateCacheSize() {
                return getCacheSize("templateCache.size", configuration.templateCache_size());
            }

            @Override
            public int getStringDeduplicationCacheSize() {
                return getCacheSize("stringDeduplicationCache.size", configuration.stringDeduplicationCache_size());
            }

            @Override
            public int getTemplateDeduplicationCacheSize() {
                return getCacheSize("templateDeduplicationCache.size", configuration.templateDeduplicationCache_size());
            }

            @Override
            public int getNodeDeduplicationCacheSize() {
                return roundToNextPowerOfTwo(getCacheSize("nodeDeduplicationCache.size", configuration.nodeDeduplicationCache_size()));
            }

            @Override
            public int getMaxFileSize() {
                return configuration.tarmk_size();
            }

            @Override
            public boolean getMemoryMapping() {
                return getMode().equals("64");
            }

            @Override
            public boolean hasCustomBlobStore() {
                return configuration.customBlobStore();
            }

            @Override
            public boolean hasCustomSegmentStore() {
                return configuration.customSegmentStore();
            }

            @Override
            public boolean hasSplitPersistence() {
                return configuration.splitPersistence();
            }

            @Override
            public boolean registerDescriptors() {
                return configuration.registerDescriptors();
            }

            @Override
            public String getRepositoryHome() {
                String repositoryHome = OsgiUtil.lookupConfigurationThenFramework(context, "repository.home");
                if (isNullOrEmpty(repositoryHome)) {
                    return "repository";
                }
                return repositoryHome;
            }

            @Override
            public long getBlobSnapshotInterval() {
                return configuration.blobTrackSnapshotIntervalInSecs();
            }

            @Override
            public long getBlobGcMaxAge() {
                return configuration.blobGcMaxAgeInSecs();
            }

            @Override
            public File getBackupDirectory() {
                String backupDirectory = configuration.repository_backup_dir();
                if (isNullOrEmpty(backupDirectory)) {
                    return new File(getRepositoryHome(), appendRole("segmentstore-backup"));
                }
                return new File(backupDirectory);
            }

            @Override
            public Whiteboard getWhiteboard() {
                return whiteboard;
            }

            @Override
            public Closer getCloser() {
                return closer;
            }

            @Override
            public Logger getLogger() {
                return logger;
            }

            @Override
            public StatisticsProvider getStatisticsProvider() {
                return statisticsProvider;
            }

            @Override
            public BlobStore getBlobStore() {
                return blobStore;
            }

            @Override
            public SegmentNodeStorePersistence getSegmentNodeStorePersistence() {
                return segmentStore;
            }

            @Override
            public BundleContext getBundleContext() {
                return context.getBundleContext();
            }

        });
    }

    private static StatisticsProvider getRoleStatisticsProvider(StatisticsProvider delegate, String role) {
        RepositoryStatistics repositoryStatistics = new RepositoryStatistics() {

            @Override
            public TimeSeries getTimeSeries(Type type) {
                return getTimeSeries(type.name(), type.isResetValueEachSecond());
            }

            @Override
            public TimeSeries getTimeSeries(String type, boolean resetValueEachSecond) {
                return delegate.getStats().getTimeSeries(addRoleToName(type, role), resetValueEachSecond);
            }
        };

        return new StatisticsProvider() {

            @Override
            public RepositoryStatistics getStats() {
                return repositoryStatistics;
            }

            @Override
            public MeterStats getMeter(String name, StatsOptions options) {
                return delegate.getMeter(addRoleToName(name, role), options);
            }

            @Override
            public CounterStats getCounterStats(String name, StatsOptions options) {
                return delegate.getCounterStats(addRoleToName(name, role), options);
            }

            @Override
            public TimerStats getTimer(String name, StatsOptions options) {
                return delegate.getTimer(addRoleToName(name, role), options);
            }

            @Override
            public HistogramStats getHistogram(String name, StatsOptions options) {
                return delegate.getHistogram(addRoleToName(name, role), options);
            }

        };
    }

    private static String addRoleToName(String name, String role) {
        return role + '.' + name;
    }

}

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
package org.apache.jackrabbit.oak.plugins.index;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.jackrabbit.guava.common.io.Closer;
import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.plugins.index.property.jmx.PropertyIndexAsyncReindex;
import org.apache.jackrabbit.oak.plugins.index.property.jmx.PropertyIndexAsyncReindexMBean;
import org.apache.jackrabbit.oak.plugins.observation.ChangeCollectorProvider;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.Clusterable;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardExecutor;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.osgi.framework.BundleContext;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.ConfigurationPolicy;
import org.osgi.service.component.annotations.Deactivate;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.metatype.annotations.AttributeDefinition;
import org.osgi.service.metatype.annotations.Designate;
import org.osgi.service.metatype.annotations.ObjectClassDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;
import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

@Component(
        configurationPolicy = ConfigurationPolicy.REQUIRE,
        service = {})
@Designate(ocd = AsyncIndexerService.Configuration.class)
public class AsyncIndexerService {

    @ObjectClassDefinition(
            name = "Apache Jackrabbit Oak Async Indexer Service",
            description = "Configures the async indexer services which performs periodic indexing of repository content"
    )
    @interface Configuration {

        @AttributeDefinition(
                cardinality = 1024,
                name = "Async Indexer Configs",
                description = "Async indexer configs in the form of <name>:<interval in secs>:<lease time out in minutes> e.g. \"async:5:15\""
        )
        String[] asyncConfigs() default {"async:5:15"};

        @AttributeDefinition(
                name = "Lease time out",
                description = "Lease timeout in minutes. AsyncIndexer would wait for this timeout period before breaking " +
                        "async indexer lease"
        )
        long leaseTimeOutMinutes() default 15L;

        @AttributeDefinition(
                name = "Failing Index Timeout (s)",
                description = "Time interval in seconds after which a failing index is considered as corrupted and " +
                        "ignored from further indexing until reindex. The default is 7 days (7 * 24 * 60 * 60 = 604800). " +
                        "To disable this set it to 0."
        )
        long failingIndexTimeoutSeconds() default 7 * 24 * 60 * 60L;

        @AttributeDefinition(
                name = "Error warn interval (s)",
                description = "Time interval in seconds after which a warning log would be logged for skipped indexes. " +
                        "This is done to avoid flooding the log in case of corrupted index."
        )
        long errorWarnIntervalSeconds() default 15 * 60;
    }

    private static final char CONFIG_SEP = ':';
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WhiteboardIndexEditorProvider indexEditorProvider = new WhiteboardIndexEditorProvider();

    @Reference
    private NodeStore nodeStore;

    @Reference(target = "(type=" + ChangeCollectorProvider.TYPE + ")")
    private ValidatorProvider validatorProvider;

    @Reference
    private StatisticsProvider statisticsProvider;

    private IndexMBeanRegistration indexRegistration;

    private final Closer closer = Closer.create();

    private WhiteboardExecutor executor;

    @Activate
    public void activate(BundleContext bundleContext, Configuration config) {
        List<AsyncConfig> asyncIndexerConfig = getAsyncConfig(config.asyncConfigs());
        Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        indexRegistration = new IndexMBeanRegistration(whiteboard);
        indexEditorProvider.start(whiteboard);
        executor = new WhiteboardExecutor();
        executor.start(whiteboard);

        TrackingCorruptIndexHandler corruptIndexHandler = createCorruptIndexHandler(config);

        for (AsyncConfig c : asyncIndexerConfig) {
            AsyncIndexUpdate task = new AsyncIndexUpdate(c.name, nodeStore, indexEditorProvider,
                    statisticsProvider, false);
            task.setCorruptIndexHandler(corruptIndexHandler);
            task.setValidatorProviders(Collections.singletonList(validatorProvider));

            long leaseTimeOutMin = config.leaseTimeOutMinutes();

            // Set lease time out = 0 for a non clusterable setup.
            if (!(nodeStore instanceof Clusterable)){
                leaseTimeOutMin = 0;
                log.info("Detected non clusterable setup. Lease checking would be disabled for async indexing");
            } else if (c.leaseTimeOutInMin != null) {
                // If lease time out is configured for a specific lane, use that.
                leaseTimeOutMin = c.leaseTimeOutInMin;
                log.info("Lease time out for {} configured as {} mins.", c.name, leaseTimeOutMin);
            } else {
                log.info("Lease time out for {} not configured explicitly. Using value {} mins configured via Lease Time out property.", c.name, leaseTimeOutMin);
            }

            task.setLeaseTimeOut(TimeUnit.MINUTES.toMillis(leaseTimeOutMin));
            indexRegistration.registerAsyncIndexer(task, c.timeIntervalInSecs);
            closer.register(task);
        }
        registerAsyncReindexSupport(whiteboard);
        log.info("Configured async indexers {} ", asyncIndexerConfig);
    }

    private void registerAsyncReindexSupport(Whiteboard whiteboard) {
        // async reindex
        String name = IndexConstants.ASYNC_REINDEX_VALUE;
        AsyncIndexUpdate task = new AsyncIndexUpdate(name, nodeStore, indexEditorProvider, statisticsProvider, true);
        PropertyIndexAsyncReindex asyncPI = new PropertyIndexAsyncReindex(task, executor);

        final Registration reg = new CompositeRegistration(
                registerMBean(whiteboard, PropertyIndexAsyncReindexMBean.class, asyncPI,
                        PropertyIndexAsyncReindexMBean.TYPE, "async"),
                registerMBean(whiteboard, IndexStatsMBean.class, task.getIndexStats(), IndexStatsMBean.TYPE, name));
        closer.register(new Closeable() {
            @Override
            public void close() throws IOException {
                reg.unregister();
            }
        });
    }

    @Deactivate
    public void deactivate() throws IOException {
        if (indexRegistration != null) {
            indexRegistration.unregister();
        }
        if (executor != null) {
            executor.stop();
            executor = null;
        }

        //Close the task *after* unregistering the jobs
        closer.close();
    }

    //~-------------------------------------------< internal >

    private TrackingCorruptIndexHandler createCorruptIndexHandler(Configuration config) {
        long failingIndexTimeoutSeconds = config.failingIndexTimeoutSeconds();
        long errorWarnIntervalSeconds = config.errorWarnIntervalSeconds();

        TrackingCorruptIndexHandler corruptIndexHandler = new TrackingCorruptIndexHandler();
        corruptIndexHandler.setCorruptInterval(failingIndexTimeoutSeconds, TimeUnit.SECONDS);
        corruptIndexHandler.setErrorWarnInterval(errorWarnIntervalSeconds, TimeUnit.SECONDS);
        corruptIndexHandler.setMeterStats(statisticsProvider.getMeter(TrackingCorruptIndexHandler.CORRUPT_INDEX_METER_NAME, StatsOptions.METRICS_ONLY));

        if (failingIndexTimeoutSeconds <= 0){
            log.info("[failingIndexTimeoutSeconds] is set to {}. Auto corrupt index isolation handling is disabled, warning log would be " +
                            "logged every {} s", failingIndexTimeoutSeconds, errorWarnIntervalSeconds);
        } else {
            log.info("Auto corrupt index isolation handling is enabled. Any async index which fails for {}s would " +
                    "be marked as corrupted and would be skipped from further indexing. A warning log would be " +
                    "logged every {} s", failingIndexTimeoutSeconds, errorWarnIntervalSeconds);
        }
        return corruptIndexHandler;
    }

    static List<AsyncConfig> getAsyncConfig(String[] configs) {
        List<AsyncConfig> result = new ArrayList<>();
        for (String config : configs) {
            int idOfEq = config.indexOf(CONFIG_SEP);
            checkArgument(idOfEq > 0, "Invalid config provided [%s]", Arrays.toString(configs));

            String[] configElements = config.split(String.valueOf(CONFIG_SEP));
            String name = configElements[0].trim();
            Long interval = configElements.length > 1 ? Long.parseLong(configElements[1].trim()) : null;
            Long leaseTimeOut = configElements.length > 2 ? Long.parseLong(configElements[2].trim()) : null;

            result.add(new AsyncConfig(name, interval, leaseTimeOut));
        }
        return result;
    }

    static class AsyncConfig {
        final String name;
        final Long timeIntervalInSecs;
        final Long leaseTimeOutInMin;

        private AsyncConfig(String name, Long timeIntervalInSecs, Long leaseTimeOutInMin) {
            this.name = AsyncIndexUpdate.checkValidName(name);
            this.timeIntervalInSecs = timeIntervalInSecs;
            this.leaseTimeOutInMin = leaseTimeOutInMin;
        }

        @Override
        public String toString() {
            return "AsyncConfig{" +
                    "name='" + name + '\'' +
                    ", timeIntervalInSecs=" + timeIntervalInSecs +
                    ", leaseTimeOutInMin=" + leaseTimeOutInMin +
                    '}';
        }
    }
}

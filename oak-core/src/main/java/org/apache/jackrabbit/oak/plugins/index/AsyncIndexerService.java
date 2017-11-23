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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.io.Closer;
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

import static com.google.common.base.Preconditions.checkArgument;
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
                description = "Async indexer configs in the form of <name>:<interval in secs> e.g. \"async:5\""
        )
        String[] asyncConfigs() default {"async:5"};
        
        @AttributeDefinition(
                name = "Lease time out",
                description = "Lease timeout in minutes. AsyncIndexer would wait for this timeout period before breaking " +
                        "async indexer lease"
        )
        int leaseTimeOutMinutes() default 15;

        @AttributeDefinition(
                name = "Failing Index Timeout (s)",
                description = "Time interval in seconds after which a failing index is considered as corrupted and " +
                        "ignored from further indexing untill reindex. To disable this set it to 0"
        )
        long failingIndexTimeoutSeconds() default 30 * 60;

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

        long leaseTimeOutMin = config.leaseTimeOutMinutes();

        if (!(nodeStore instanceof Clusterable)){
            leaseTimeOutMin = 0;
            log.info("Detected non clusterable setup. Lease checking would be disabled for async indexing");
        }

        TrackingCorruptIndexHandler corruptIndexHandler = createCorruptIndexHandler(config);

        for (AsyncConfig c : asyncIndexerConfig) {
            AsyncIndexUpdate task = new AsyncIndexUpdate(c.name, nodeStore, indexEditorProvider,
                    statisticsProvider, false);
            task.setCorruptIndexHandler(corruptIndexHandler);
            task.setValidatorProviders(Collections.singletonList(validatorProvider));
            task.setLeaseTimeOut(TimeUnit.MINUTES.toMillis(leaseTimeOutMin));

            indexRegistration.registerAsyncIndexer(task, c.timeIntervalInSecs);
            closer.register(task);
        }
        registerAsyncReindexSupport(whiteboard);
        log.info("Configured async indexers {} ", asyncIndexerConfig);
        log.info("Lease time: {} mins and AsyncIndexUpdate configured with {}", leaseTimeOutMin, validatorProvider.getClass().getName());
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
        List<AsyncConfig> result = Lists.newArrayList();
        for (String config : configs) {
            int idOfEq = config.indexOf(CONFIG_SEP);
            checkArgument(idOfEq > 0, "Invalid config provided [%s]", Arrays.toString(configs));

            String name = config.substring(0, idOfEq).trim();
            long interval = Long.parseLong(config.substring(idOfEq + 1));
            result.add(new AsyncConfig(name, interval));
        }
        return result;
    }

    static class AsyncConfig {
        final String name;
        final long timeIntervalInSecs;

        private AsyncConfig(String name, long timeIntervalInSecs) {
            this.name = AsyncIndexUpdate.checkValidName(name);
            this.timeIntervalInSecs = timeIntervalInSecs;
        }

        @Override
        public String toString() {
            return "AsyncConfig{" +
                    "name='" + name + '\'' +
                    ", timeIntervalInSecs=" + timeIntervalInSecs +
                    '}';
        }
    }
}

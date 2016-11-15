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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.oak.commons.PropertiesUtil;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardIndexEditorProvider;
import org.osgi.framework.BundleContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkArgument;

@Component(
        policy = ConfigurationPolicy.REQUIRE,
        metatype = true,
        label = "Apache Jackrabbit Oak Async Indexer Service",
        description = "Configures the async indexer services which performs periodic indexing of repository content"
)
public class AsyncIndexerService {
    @Property(
            value = {
                    "async:5"
            },
            cardinality = 1024,
            label = "Async Indexer Configs",
            description = "Async indexer configs in the form of <name>:<interval in secs> e.g. \"async:5\""
    )
    private static final String PROP_ASYNC_CONFIG = "asyncConfigs";

    private static final int PROP_LEASE_TIMEOUT_DEFAULT = 15;
    @Property(
            intValue = PROP_LEASE_TIMEOUT_DEFAULT,
            label = "Lease time out",
            description = "Lease timeout in minutes. AsyncIndexer would wait for this timeout period before breaking " +
                    "async indexer lease"
    )
    private static final String PROP_LEASE_TIME_OUT = "leaseTimeOutMinutes";

    private static final char CONFIG_SEP = ':';
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final WhiteboardIndexEditorProvider indexEditorProvider = new WhiteboardIndexEditorProvider();
    @Reference
    private NodeStore nodeStore;
    private IndexMBeanRegistration indexRegistration;

    @Activate
    public void activate(BundleContext bundleContext, Map<String, Object> config) {
        List<AsyncConfig> asyncIndexerConfig = getAsyncConfig(PropertiesUtil.toStringArray(config.get
                (PROP_ASYNC_CONFIG), new String[0]));
        Whiteboard whiteboard = new OsgiWhiteboard(bundleContext);
        indexRegistration = new IndexMBeanRegistration(whiteboard);
        indexEditorProvider.start(whiteboard);

        long leaseTimeOutMin = PropertiesUtil.toInteger(config.get(PROP_LEASE_TIME_OUT), PROP_LEASE_TIMEOUT_DEFAULT);

        for (AsyncConfig c : asyncIndexerConfig) {
            AsyncIndexUpdate task = new AsyncIndexUpdate(c.name, nodeStore, indexEditorProvider);
            task.setLeaseTimeOut(TimeUnit.MINUTES.toMillis(leaseTimeOutMin));
            indexRegistration.registerAsyncIndexer(task, c.timeIntervalInSecs);
        }
        log.info("Configured async indexers {} ", asyncIndexerConfig);
        log.info("Lease time: {} mins", leaseTimeOutMin);
    }

    @Deactivate
    public void deactivate() {
        if (indexRegistration != null) {
            indexRegistration.unregister();
        }
    }

    //~-------------------------------------------< internal >

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
            this.name = name;
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

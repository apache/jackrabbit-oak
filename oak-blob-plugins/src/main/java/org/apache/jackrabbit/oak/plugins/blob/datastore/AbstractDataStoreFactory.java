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

package org.apache.jackrabbit.oak.plugins.blob.datastore;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closer;
import org.apache.commons.io.IOUtils;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Reference;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.osgi.OsgiWhiteboard;
import org.apache.jackrabbit.oak.spi.blob.CompositeDataStoreAware;
import org.apache.jackrabbit.oak.spi.blob.DataStoreProvider;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Dictionary;
import java.util.List;
import java.util.Map;

import static org.apache.jackrabbit.oak.osgi.OsgiUtil.lookupConfigurationThenFramework;

@Component(componentAbstract = true)
public abstract class AbstractDataStoreFactory {
    private static final String ROLE = DataStoreProvider.ROLE;

    private static final Logger log = LoggerFactory.getLogger(AbstractDataStoreFactory.class.getName());

    protected Closer closer = Closer.create();

    @Reference
    private StatisticsProvider statisticsProvider = StatisticsProvider.NOOP;

    @Activate
    public void activate(ComponentContext context) throws IOException {
        final String role = lookupConfigurationThenFramework(context, ROLE);

        // Data stores being created via the factory MUST have a role configured
        if (null != role) {
            log.info("activate: {} with role {} starting via factory", getClass().getSimpleName(), role);

            Map<String, Object> config = getConfigFromContext(context);

            List<String> desc = Lists.newArrayList(getDescription());
            desc.add(String.format("%s=%s", ROLE, role));
            OsgiWhiteboard whiteboard = new OsgiWhiteboard(context.getBundleContext());

            final DataStore dataStore = createDataStore(context, config);
            if (null != dataStore) {
                if (dataStore instanceof CompositeDataStoreAware) {
                    ((CompositeDataStoreAware)dataStore).setIsDelegate(true);
                }
                Map<String, Object> props = Maps.newConcurrentMap();
                props.putAll(config);
                props.putIfAbsent(DataStoreProvider.ROLE, role);

                closer.register(asCloseable(whiteboard.register(
                        DataStoreProvider.class,
                        new DataStoreProvider() {
                            @Override public String getRole() { return role; }
                            @Override public DataStore getDataStore() { return dataStore; }
                        },
                        props
                )));

                log.info("Registered DataStoreProvider of type {} with role {}", dataStore.getClass().getSimpleName(), role);
            }
        }
        else {
            log.warn("Could not register DataStoreProvider - no role defined");
        }
    }

    @Deactivate
    public void deactivate() {
        if (closer != null) {
            IOUtils.closeQuietly(closer);
            closer = null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getConfigFromContext(ComponentContext context) {
        Dictionary<String, Object> d = context.getProperties();
        return Maps.toMap(Iterators.forEnumeration(d.keys()), d::get);
    }

    protected abstract DataStore createDataStore(ComponentContext context, Map<String, Object> config);

    protected StatisticsProvider getStatisticsProvider() { return statisticsProvider; }

    protected String[] getDescription() {
        return new String[] { "type=unknown" };
    }

    protected static Closeable asCloseable(Registration r) {
        return new Closeable() {
            @Override public void close() {
                r.unregister();
            }
        };
    }

    protected static Closeable asCloseable(ServiceRegistration r) {
        return new Closeable() {
            @Override public void close() {
                r.unregister();
            }
        };
    }
}

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
package org.apache.jackrabbit.oak.osgi;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

/**
 * This index provider combines all indexes of all available OSGi index
 * providers.
 */
public class OsgiIndexProvider implements ServiceTrackerCustomizer, QueryIndexProvider {

    private BundleContext context;

    private ServiceTracker tracker;

    private final Map<ServiceReference, QueryIndexProvider> providers =
        new HashMap<ServiceReference, QueryIndexProvider>();

    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        tracker = new ServiceTracker(
                bundleContext, QueryIndexProvider.class.getName(), this);
        tracker.open();
    }

    public void stop() throws Exception {
        tracker.close();
    }

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof QueryIndexProvider) {
            QueryIndexProvider provider = (QueryIndexProvider) service;
            providers.put(reference, provider);
            return service;
        } else {
            context.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        providers.remove(reference);
        context.ungetService(reference);
    }

    @Override
    public List<? extends QueryIndex> getQueryIndexes(NodeStore nodeStore) {
        if (providers.isEmpty()) {
            return Collections.emptyList();
        } else if (providers.size() == 1) {
            return providers.entrySet().iterator().next().getValue().getQueryIndexes(nodeStore);
        } else {
            // TODO combine indexes
            return null;
        }
    }

}

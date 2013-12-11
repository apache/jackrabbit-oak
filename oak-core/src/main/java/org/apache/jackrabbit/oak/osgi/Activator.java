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
package org.apache.jackrabbit.oak.osgi;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.api.jmx.CacheStatsMBean;
import org.apache.jackrabbit.oak.kernel.KernelNodeStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.apache.jackrabbit.oak.spi.whiteboard.Registration;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.framework.ServiceRegistration;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

import static org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils.registerMBean;

public class Activator implements BundleActivator, ServiceTrackerCustomizer {

    private BundleContext context;

    private ServiceTracker microKernelTracker;

    private Whiteboard whiteboard;

    private final Map<ServiceReference, ServiceRegistration> services =
            new HashMap<ServiceReference, ServiceRegistration>();

    private final List<Registration> registrations = new ArrayList<Registration>();

    //----------------------------------------------------< BundleActivator >---

    @Override
    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        whiteboard = new OsgiWhiteboard(bundleContext);

        microKernelTracker = new ServiceTracker(context, MicroKernel.class.getName(), this);
        microKernelTracker.open();
    }

    @Override
    public void stop(BundleContext bundleContext) throws Exception {
        microKernelTracker.close();

        for (Registration r : registrations) {
            r.unregister();
        }
    }

    //-------------------------------------------< ServiceTrackerCustomizer >---

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof MicroKernel) {
            MicroKernel kernel = (MicroKernel) service;
            KernelNodeStore store = new KernelNodeStore(kernel);
            services.put(reference, context.registerService(
                    NodeStore.class.getName(),
                    store,
                    new Properties()));
            registrations.add(registerMBean(whiteboard, CacheStatsMBean.class,
                store.getCacheStats(), CacheStatsMBean.TYPE, store.getCacheStats().getName()));
        }
        return service;
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        services.remove(reference).unregister();
        context.ungetService(reference);
    }

}

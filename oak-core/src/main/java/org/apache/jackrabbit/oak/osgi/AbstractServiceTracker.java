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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

/**
 * <code>AbstractServiceTracker</code> is a base class for the various OSGi based
 * providers.
 */
public abstract class AbstractServiceTracker<T> implements ServiceTrackerCustomizer {

    private BundleContext context;

    private ServiceTracker tracker;

    private final Map<ServiceReference, T> services =
            new HashMap<ServiceReference, T>();

    private final Class<T> serviceClass;

    public AbstractServiceTracker(Class<T> serviceClass) {
        this.serviceClass = serviceClass;
    }

    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        tracker = new ServiceTracker(
                bundleContext, serviceClass.getName(), this);
        tracker.open();
    }

    public void stop() throws Exception {
        tracker.close();
    }

    /**
     * Returns all services of type <code>T</code> currently available.
     *
     * @return services currently available.
     */
    protected List<T> getServices() {
        synchronized (this) {
            return new ArrayList<T>(services.values());
        }
    }

    //------------------------< ServiceTrackerCustomizer >----------------------

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);

        if (serviceClass.isInstance(service)) {
            synchronized (this) {
                services.put(reference, (T) service);
            }
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
        synchronized (this) {
            services.remove(reference);
        }
        context.ungetService(reference);
    }
}
